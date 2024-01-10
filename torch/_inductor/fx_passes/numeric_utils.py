import logging
import traceback

import torch
import torch.optim as optim

logger: logging.Logger = logging.getLogger(__name__)


# We compare the numerical results before and after Optimus transformation
# to make sure the numerical results are the same.
def compare_dict_tensors(dict_base, dict_control, precision):
    if len(set(dict_base.keys())) != len(set(dict_control.keys())):
        logger.info(
            f"Mismatch parameter name. pt2.keys(){dict_base.keys()}, pt2+Optimus.keys() {dict_control.keys()}"
        )
        return False
    is_allclose = True
    for key in dict_base.keys():
        if key not in dict_control:
            logger.warning(
                f"Mismatch parameter name. {key} does not exist in pt2 with Optimus."
            )
        # The default model precision is ft32, unless users manually set it to fp16.
        # Some parameters have `None`, and not every param has a valid .grad field, we skip them
        if dict_base[key] is None or dict_control[key] is None:
            continue
        if not torch.allclose(
            dict_base[key],
            dict_control[key],
            rtol=precision,
            atol=precision,
            equal_nan=True,
        ):
            logger.warning(
                f"Key {key}, value in pt2 {dict_base[key]}, value in pt2 with Optimus {dict_control[key]}"
            )
            is_allclose = False
    return is_allclose


def compare_tuple_tensors(tuple_base, tuple_control, precision):
    if len(tuple_base) != len(tuple_control):
        logger.warning(
            f"Mismatch tuple length. pt2.length:{len(tuple_base)}, pt2+Optimus.length:{len(tuple_control)}"
        )
        return False
    is_allclose = True
    for i in range(len(tuple_base)):
        # The default model precision is ft32, unless users manually set it to fp16.
        # Some parameters have `None`, we skip them
        if tuple_base[i] is None or tuple_control[i] is None:
            continue
        if not torch.allclose(
            tuple_base[i],
            tuple_control[i],
            rtol=precision,
            atol=precision,
            equal_nan=True,
        ):
            logger.warning(
                f"value in pt2 {tuple_base[i]}, value in pt2 with Optimus {tuple_control[i]}"
            )
            is_allclose = False
    return is_allclose


def compare_parameters(model_base, model_control, precision):
    return compare_dict_tensors(
        dict(model_base.named_parameters()),
        dict(model_control.named_parameters()),
        precision,
    )


def compare_forward_output(pred_base, pred_control, precision):
    return compare_tuple_tensors(
        pred_base,
        pred_control,
        precision,
    )


def compare_gradients(model_base, model_control, precision):
    grad_base = {key: param.grad for key, param in model_base.named_parameters()}
    grad_pt2 = {key: param.grad for key, param in model_control.named_parameters()}
    return compare_dict_tensors(
        grad_base,
        grad_pt2,
        precision,
    )


def run_model(
    model_base, model_control, model_input, num_iteration=10, precision=1e-32
):
    for i in range(num_iteration):
        logger.info(f"start {i} iteration")
        pred_base = model_base(*model_input)
        pred_control = model_control(*model_input)

        res = compare_parameters(model_base, model_control, precision)
        logger.info(f"compare parameters. Numerical result :{res}")

        res = compare_forward_output(pred_base, pred_control, precision)
        logger.info(f"compare loss/predict. Numerical result :{res}")
        # tensor may not have a grad_fn
        try:
            _ = pred_base[0].sum().backward(retain_graph=True)
            _ = pred_control[0].sum().backward(retain_graph=True)
            res = compare_gradients(model_base, model_control, precision)
            logger.info(f"compare param grad. Numerical result :{res}")
        except Exception as e:
            logger.exception(f"Exception {e} when compare gradients")
            traceback.print_exc()

        # optimizer
        # check the parameter list is not empty
        if (
            len(dict(model_base.named_parameters())) > 0
            and len(dict(model_control.named_parameters())) > 0
        ):
            optimizer_base = optim.SGD(
                [param for name, param in model_base.named_parameters()], lr=0.01
            )
            optimizer_base.step()

            optimizer_optimus = optim.SGD(
                [param for name, param in model_control.named_parameters()], lr=0.01
            )
            optimizer_optimus.step()

            res = compare_parameters(model_base, model_control, precision)
            logger.info(
                f"compare parameters with optimizer added. Numerical result :{res}"
            )
        else:
            logger.warning(
                f"no parameter with optimizer to compare since the length of pt2 model is {len(dict(model_base.named_parameters()))}"
                f" and the length of pt2+Optimus model is {len(dict(model_control.named_parameters()))}"
            )
