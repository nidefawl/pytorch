# Delete old branches
import os
import json
import re
from datetime import datetime
from pathlib import Path
from gitutils import GitRepo
from github_utils import gh_graphql, gh_fetch_json_dict
from typing import Any, Dict, List

SEC_IN_DAY = 24 * 60 * 60
CLOSED_PR_RETENTION = 30 * SEC_IN_DAY
NO_PR_RETENTION = 1.5 * 365 * SEC_IN_DAY
PR_WINDOW = 90 * SEC_IN_DAY  # Set to None to look at all PRs (may take a lot of tokens)
REPO_OWNER = "pytorch"
REPO_NAME = "pytorch"
PR_BODY_MAGIC_STRING = "do-not-delete-branch"
ESTIMATED_TOKENS = [0]

TOKEN = os.environ["GITHUB_TOKEN"]
if not TOKEN:
    raise Exception("GITHUB_TOKEN is not set")

REPO_ROOT = Path(__file__).parent.parent.parent

GRAPHQL_PRS_QUERY = """
query ($owner: String!, $repo: String!, $cursor: String) {
  repository(owner: $owner, name: $repo) {
    pullRequests(
      first: 100
      after: $cursor
      orderBy: {field: UPDATED_AT, direction: DESC}
    ) {
      totalCount
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
        headRefName
        number
        updatedAt
        state
        body
      }
    }
  }
}
"""


def is_protected(branch):
    ESTIMATED_TOKENS[0] += 1
    res = gh_fetch_json_dict(f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/branches/{branch}")
    return res['protected']

def convert_gh_timestamp(date):
    return datetime.strptime(date, "%Y-%m-%dT%H:%M:%SZ").timestamp()


def get_branches(repo):
    # Query locally for branches, group by branch base name (e.g. gh/blah/base -> gh/blah), and get the most recent branch
    git_response = repo._run_git("for-each-ref", "--sort=creatordate" , "--format=%(refname) %(committerdate:iso-strict)", "refs/remotes/origin")
    branches_by_base_name = {}
    for line in git_response.splitlines():
        branch, date = line.split(" ")
        branch = branch_base_name = re.match(r"refs/remotes/origin/(.*)", branch).group(1)
        date = datetime.fromisoformat(date).timestamp()
        if x := re.match(r"(gh\/.+)\/(head|base|orig)", branch):
            branch_base_name = x.group(1)
        if branch_base_name not in branches_by_base_name:
            branches_by_base_name[branch_base_name] = [date, [branch]]
        else:
            branches_by_base_name[branch_base_name][1].append(branch)
            if date > branches_by_base_name[branch_base_name][0]:
                branches_by_base_name[branch_base_name][0] = date
    return branches_by_base_name


def get_prs():
    now = datetime.now().timestamp()

    pr_infos: List[Dict[str, Any]] = []

    hasNextPage = True
    endCursor = None
    while hasNextPage:
        ESTIMATED_TOKENS[0] += 1
        res = gh_graphql(
            GRAPHQL_PRS_QUERY,
            owner="pytorch",
            repo="pytorch",
            cursor=endCursor
        )
        info = res['data']["repository"]["pullRequests"]
        pr_infos.extend(info["nodes"])
        hasNextPage = info["pageInfo"]["hasNextPage"]
        endCursor = info["pageInfo"]["endCursor"]
        if PR_WINDOW and now - convert_gh_timestamp(pr_infos[-1]["updatedAt"]) > PR_WINDOW:
            break


    # Get the most recent PR for each branch base (group gh together)
    prs_by_branch_base = {}
    for pr in pr_infos:
        pr['updatedAt'] = convert_gh_timestamp(pr['updatedAt'])
        branch_base_name = pr['headRefName']
        if x := re.match(r"(gh\/.+)\/(head|base|orig)", branch_base_name):
            branch_base_name = x.group(1)
        if branch_base_name not in prs_by_branch_base:
            prs_by_branch_base[branch_base_name] = pr
        else:
            if pr['updatedAt'] > prs_by_branch_base[branch_base_name]['updatedAt']:
                prs_by_branch_base[branch_base_name] = pr
    return prs_by_branch_base


def delete_branch(repo, branch):
    repo._run_git("push", "origin", "-d", branch)

def delete_branches():
    now = datetime.now().timestamp()
    branches = get_branches(GitRepo(REPO_ROOT, "origin", debug=True))
    prs_by_branch = get_prs()
    with open("t.txt", "w") as f:
        f.write(json.dumps(prs_by_branch, indent=2))
    with open("t.txt") as f:
        prs_by_branch = json.load(f)

    delete = []
    # Do not delete if:
    # * associated PR is open, closed but updated recently, or contains the magic string
    # * no associated PR and branch was updated in last 1.5 years
    # * is protected
    for branch, (date, sub_branches) in reversed(branches.items()):
        if pr := prs_by_branch.get(branch):
            if pr['state'] == "OPEN":
                continue
            if pr['state'] == "CLOSED" and now - pr['updatedAt'] < CLOSED_PR_RETENTION:
                continue
            if PR_BODY_MAGIC_STRING in pr['body']:
                continue
        elif now - date < NO_PR_RETENTION:
            continue
        if any(is_protected(sub_branch) for sub_branch in sub_branches):
            continue
        for sub_branch in sub_branches:
            delete.append(sub_branch)
        if ESTIMATED_TOKENS[0] > 400:
            print("Estimated tokens exceeded, exiting")
            break

    print("To delete:")
    for branch in delete:
        print(branch)

    print(len(delete))

if __name__ == "__main__":
    delete_branches()
