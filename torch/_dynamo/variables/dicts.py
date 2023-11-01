import collections
import dataclasses
import enum
import functools
import inspect
import sys
from typing import Any, Dict, List, Optional

from torch._subclasses.fake_tensor import is_fake

from .. import variables
from ..bytecode_transformation import create_call_function, create_instruction
from ..eval_frame import skip_code

from ..exc import unimplemented
from ..guards import GuardBuilder, make_dupe_guard
from ..source import AttrSource, GetItemSource
from ..utils import iter_contains, specialize_symnode
from .base import MutableLocal, VariableTracker
from .constant import ConstantVariable


# Note: [Adding a new supported class the keys of ConstDictVarialble]
# You'll need to add it to:
# - `is_hashable_python_var` in this file
# - `is_hashable` in this file
# - `const_repr` in util.py, and perhaps modify DICT_KEYS in guards.py


def is_hashable_python_var(x):
    from torch import Tensor

    # Note: Keep me in sync with is_hashable!
    # Even better, we should have a map of functions connecting the two
    from ..allowed_functions import is_builtin_callable

    return (
        ConstantVariable.is_literal(x)
        or isinstance(x, (Tensor, enum.Enum))
        or is_builtin_callable(x)
        or (isinstance(x, tuple) and all(is_hashable_python_var(e) for e in x))
    )


def is_hashable(x):
    # Keep me in sync with is_hashable_python_var!
    # Even better, we should have a map of functions connecting the two
    if isinstance(x, variables.TensorVariable):
        # Tensors are hashable if they have an example_value (a fake tensor)
        # Most VT's should have one.
        # It'd be nice if at some point we could assert that they all have one
        return x.as_proxy().node.meta.get("example_value") is not None
    elif isinstance(x, variables.TupleVariable):
        return all(is_hashable(e) for e in x.items)
    else:
        return isinstance(
            x,
            (
                variables.BuiltinVariable,
                variables.SymNodeVariable,
                variables.ConstantVariable,
                variables.EnumVariable,
            ),
        )


class ConstDictVariable(VariableTracker):
    class _HashableTracker:
        """
        Auxiliary opaque internal class that wraps a VariableTracker and makes it hashable
        This should not be seen or touched by anything outside of ConstDictVariable and its children
        Note that it's also fine to put VTs into dictionaries and sets, but doing so does not take into account aliasing
        """

        def __init__(self, vt):
            # We specialize SymNodes
            vt = specialize_symnode(vt)
            assert is_hashable(vt), type(vt)
            self.vt = vt

        @property
        def underlying_value(self):
            if isinstance(self.vt, variables.TensorVariable):
                x = self.vt.as_proxy().node.meta["example_value"]
            elif isinstance(self.vt, variables.TupleVariable):
                Hashable = ConstDictVariable._HashableTracker
                x = tuple(Hashable(e).underlying_value for e in self.vt.items)
            else:
                x = self.vt.as_python_constant()
            return x

        def __hash__(self):
            return hash(self.underlying_value)

        @staticmethod
        def _eq_impl(a: VariableTracker, b: VariableTracker):
            # TODO: Put this in utils and share it between variables/builtin.py and here
            if type(a) != type(b):
                return False
            elif isinstance(a, tuple):
                return len(a) == len(b) and all(_eq_impl(u, v) for u, v in zip(a, b))
            elif is_fake(a):
                return a is b
            else:
                return a == b

        def __eq__(self, other: object) -> bool:
            Hashable = ConstDictVariable._HashableTracker
            if not isinstance(other, Hashable):
                return False
            return Hashable._eq_impl(self.underlying_value, other.underlying_value)

    def __init__(self, items, user_cls=dict, **kwargs):
        super().__init__(**kwargs)

        Hashable = ConstDictVariable._HashableTracker

        # Keys will just be HashableTrackers when cloning, in any other case they'll be VariableTrackers
        def make_hashable(key):
            return key if isinstance(key, Hashable) else Hashable(key)

        assert all(
            isinstance(x, (VariableTracker, Hashable))
            and isinstance(v, VariableTracker)
            for x, v in items.items()
        )
        self.items = {make_hashable(x): v for x, v in items.items()}
        self.guards.update(
            VariableTracker.propagate(
                [x.vt for x in self.items.keys()], self.items.values()
            )["guards"]
        )
        self.user_cls = user_cls

    def as_proxy(self):
        return {k.vt.as_proxy(): v.as_proxy() for k, v in self.items.items()}

    def as_python_constant(self):
        return {
            k.vt.as_python_constant(): v.as_python_constant()
            for k, v in self.items.items()
        }

    def keys_as_python_constant(self):
        return {k.vt.as_python_constant(): v for k, v in self.items.items()}

    def python_type(self):
        return self.user_cls

    def __contains__(self, obj):
        # If it's literal we wrap it for you
        if ConstantVariable.is_literal(obj):
            obj = ConstantVariable.create(obj)

        if isinstance(obj, VariableTracker):
            key = ConstDictVariable._HashableTracker(obj)
            return key in self.items
        else:
            # If it's hashable and it's not a constant, you should wrap it first!
            assert not is_hashable_python_var(x)
            return False

    def reconstruct(self, codegen):
        # instructions to load collections.OrderedDict if necessary
        if self.user_cls is collections.OrderedDict:
            codegen.extend_output(
                [
                    codegen.create_load_python_module(collections, True),
                    codegen.create_load_attr("OrderedDict"),
                ]
            )
        # instructions to build the dict keys and values
        for key, value in self.items.items():
            codegen(key.vt)
            codegen(value)
        # BUILD_MAP and calling collections.OrderedDict if necessary
        if self.user_cls is collections.OrderedDict:
            return [
                create_instruction("BUILD_MAP", arg=len(self.items)),
                *create_call_function(1, False),
            ]
        # BUILD_MAP only if user_cls is dict
        else:
            return [create_instruction("BUILD_MAP", arg=len(self.items))]

    def getitem_const(self, arg: VariableTracker):
        key = ConstDictVariable._HashableTracker(arg)
        return self.items[key].add_options(self, arg)

    def call_method(
        self,
        tx,
        name,
        args: "List[VariableTracker]",
        kwargs: "Dict[str, VariableTracker]",
    ) -> "VariableTracker":
        from . import ConstantVariable, TupleVariable

        options = VariableTracker.propagate(self, args, kwargs.values())
        Hashable = ConstDictVariable._HashableTracker

        arg_hashable = args and is_hashable(args[0])

        if name == "__getitem__":
            return self.getitem_const(args[0])

        elif name == "items":
            assert not (args or kwargs)
            items = [TupleVariable([k.vt, v], **options) for k, v in self.items.items()]
            return TupleVariable(items, **options)
        elif name == "keys":
            assert not (args or kwargs)
            return SetVariable(
                [k.vt for k in self.items.keys()],
                mutable_local=MutableLocal(),
                **options,
            )
        elif name == "values":
            assert not (args or kwargs)
            return TupleVariable(list(self.items.values()), **options)
        elif name == "__len__":
            assert not (args or kwargs)
            return ConstantVariable.create(len(self.items), **options)
        elif name == "__setitem__" and arg_hashable and self.mutable_local:
            assert not kwargs and len(args) == 2
            k = Hashable(args[0])

            newval = collections.OrderedDict(self.items)
            newval[k] = args[1]

            return tx.replace_all(
                self,
                self.modifed(newval, **options),
            )
        elif (
            name in ("pop", "get")
            and arg_hashable
            and Hashable(args[0]) not in self.items
            and len(args) == 2
        ):
            # missing item, return the default value
            return args[1].add_options(options)
        elif name == "pop" and arg_hashable and self.mutable_local:
            newval = collections.OrderedDict(self.items)
            result = newval.pop(Hashable(args[0]))
            tx.replace_all(self, self.modifed(newval, **options))
            return result.add_options(options)
        elif (
            name == "update"
            and args
            and isinstance(args[0], ConstDictVariable)
            and self.mutable_local
        ):
            newval = collections.OrderedDict(self.items)
            newval.update(args[0].items)
            result = self.modifed(newval, **options)
            return tx.replace_all(self, result)
        elif (
            name in ("get", "__getattr__")
            and arg_hashable
            and Hashable(args[0]) in self.items
        ):
            result = self.items[Hashable(args[0])]
            return result.add_options(options)
        elif name == "__contains__" and args:
            return ConstantVariable.create(
                arg_hashable and Hashable(args[0]) in self.items,
                **options,
            )
        else:
            return super().call_method(tx, name, args, kwargs)

    def modifed(self, items, **options):
        """a copy of self with different items"""
        return self.clone(items=items, **options)

    def unpack_var_sequence(self, tx):
        return [x.vt for x in self.items.keys()]


class DefaultDictVariable(ConstDictVariable):
    def __init__(self, items, user_cls, default_factory=None, **kwargs):
        super().__init__(items, user_cls, **kwargs)
        assert user_cls is collections.defaultdict
        self.default_factory = default_factory

    def is_python_constant(self):
        # Return false for unsupported defaults. This ensures that a bad handler
        # path is not taken in BuiltinVariable for getitem.
        if self.default_factory not in [list, tuple, dict] and not self.items:
            return False
        return super().is_python_constant()

    @staticmethod
    def is_supported_arg(arg):
        if isinstance(arg, variables.BuiltinVariable):
            return arg.fn in [list, tuple, dict]
        else:
            return isinstance(arg, variables.functions.BaseUserFunctionVariable)

    def call_method(
        self,
        tx,
        name,
        args: "List[VariableTracker]",
        kwargs: "Dict[str, VariableTracker]",
    ) -> "VariableTracker":
        options = VariableTracker.propagate(self, args, kwargs.values())
        Hashable = ConstDictVariable._HashableTracker

        if name == "__getitem__":
            k = Hashable(args[0])

            if k in self.items:
                return self.getitem_const(args[0])
            else:
                if self.default_factory is None:
                    raise KeyError(f"{k}")
                else:
                    new_val = collections.OrderedDict(self.items)
                    default_var = self.default_factory.call_function(tx, [], {})
                    new_val[k] = default_var
                    tx.replace_all(self, self.modifed(new_val, **options))
                    return default_var
        else:
            return super().call_method(tx, name, args, kwargs)


class SetVariable(VariableTracker):
    @dataclasses.dataclass
    class SetElement:
        vt: VariableTracker
        underlying_value: Any

        def __hash__(self) -> int:
            return hash(self.underlying_value)

        def __eq__(self, other: object) -> bool:
            if not isinstance(other, SetVariable.SetElement):
                return False
            if isinstance(self.vt, variables.TensorVariable):
                return self.underlying_value is other.underlying_value
            else:
                return self.underlying_value == other.underlying_value

    def __init__(
        self,
        items: List[VariableTracker],
        regen_guards=True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        # Note - Set is still backed by a list, because we want set behavior over the contents,
        assert isinstance(items, list)
        assert all(isinstance(x, VariableTracker) for x in items)

        self.items = []
        self._add(items)

        # Sometimes, we know that we have passed in the guards from the items in the set
        if regen_guards:
            self.guards.update(VariableTracker.propagate(items)["guards"])

    def as_proxy(self):
        return [x.as_proxy() for x in self.items]

    def python_type(self):
        return set

    def reconstruct(self, codegen):
        codegen.load_import_from("builtins", "set")
        codegen.foreach(self.items)
        return [
            create_instruction("BUILD_SET", arg=len(self.items))
        ] + create_call_function(1, True)

    # Note - this is only used for producing a set
    def _as_set_element(self, vt):
        from .base import VariableTracker
        from .misc import MethodWrapperVariable
        from .tensor import TensorVariable

        assert isinstance(vt, VariableTracker)

        if isinstance(vt, TensorVariable):
            fake_tensor = vt.as_proxy().node.meta.get("example_value")
            if fake_tensor is None:
                unimplemented(
                    "Cannot check Tensor object identity without its fake value"
                )
            return SetVariable.SetElement(vt, fake_tensor)
        if isinstance(vt, ConstantVariable):
            return SetVariable.SetElement(vt, vt.value)
        if isinstance(vt, MethodWrapperVariable):
            return SetVariable.SetElement(vt, vt.as_python_constant())

        unimplemented(f"Sets with {type(vt)} NYI")

    @property
    def _underlying_items(self):
        underlying_items = set()
        for current_item in self.items:
            assert (
                current_item not in underlying_items
            ), "Items modeling set invariant violated"
            underlying_items.add(self._as_set_element(current_item))
        return underlying_items

    def _add(self, item):
        underlying_items = self._underlying_items

        if isinstance(item, (list, set)):
            items_to_add = item
        else:
            items_to_add = [item]

        for item_to_add in items_to_add:
            set_element = self._as_set_element(item_to_add)
            if set_element not in underlying_items:
                underlying_items.add(set_element)
                self.items.append(set_element.vt)
            else:
                for e in underlying_items:
                    if hash(set_element) == hash(e):
                        alias_guard = make_dupe_guard(
                            e.vt.source, set_element.vt.source
                        )
                        if alias_guard:
                            e.vt = e.vt.add_guards(
                                {e.vt.source.make_guard(alias_guard)}
                            )

        return self.items

    def call_method(
        self,
        tx,
        name,
        args: List[VariableTracker],
        kwargs: Dict[str, VariableTracker],
    ) -> "VariableTracker":
        options = VariableTracker.propagate(self, args, kwargs.values())
        # Somewhat duplicative of CommonListMethodsVariable - but better than to violate substitution
        # principles and end up with things like direct item access attempts on a set, or
        # getitem sources.
        if name == "add" and args and self.mutable_local:
            assert not kwargs
            item = args[0]
            result = SetVariable(
                self._add(item),
                mutable_local=self.mutable_local,
                regen_guards=False,
                **options,
            )
            tx.replace_all(self, result)
            return ConstantVariable.create(None)
        elif name == "pop" and self.mutable_local:
            assert not kwargs
            assert not args
            items = list(self.items)
            result = items.pop()
            tx.replace_all(
                self,
                SetVariable(items, regen_guards=False, **options),
            )
            return result
        elif name == "__len__":
            return ConstantVariable.create(len(self.items)).add_options(options)
        elif name == "__contains__":
            assert len(args) == 1
            assert not kwargs
            return iter_contains(
                self.items, args[0], tx, options, check_tensor_identity=True
            )
        else:
            return super().call_method(tx, name, args, kwargs)

    def getitem_const(self, arg: VariableTracker):
        raise RuntimeError("Illegal to getitem on a set")

    def as_python_constant(self):
        return self.python_type()([x.as_python_constant() for x in self.items])

    def unpack_var_sequence(self, tx):
        return [x.add_options(self) for x in self.items]


def _is_matching_transformers_cls(cls) -> bool:
    if not cls.__module__.startswith("transformers."):
        return False

    try:
        from transformers.file_utils import ModelOutput

        return issubclass(cls, ModelOutput)
    except ImportError:
        return False


def _is_matching_diffusers_cls(cls) -> bool:
    if not cls.__module__.startswith("diffusers."):
        return False

    try:
        from diffusers.utils import BaseOutput

        return issubclass(cls, BaseOutput)
    except ImportError:
        return False


class DataClassVariable(ConstDictVariable):
    """
    This is a bit of a hack to deal with
    transformers.file_utils.ModelOutput() from huggingface.

    ModelOutput causes trouble because it a a mix of a dataclass and a
    OrderedDict and it calls super() methods implemented in C.
    """

    # ModelOutput() excludes None, though generic datclasses don't
    include_none = False

    @staticmethod
    @functools.lru_cache(None)
    def _patch_once():
        try:
            from transformers.file_utils import ModelOutput

            for obj in ModelOutput.__dict__.values():
                if callable(obj):
                    skip_code(obj.__code__)
        except ImportError:
            pass

        try:
            from diffusers.utils import BaseOutput

            for obj in BaseOutput.__dict__.values():
                if callable(obj):
                    skip_code(obj.__code__)
        except ImportError:
            pass

    @staticmethod
    def is_matching_cls(cls):
        return _is_matching_transformers_cls(cls) or _is_matching_diffusers_cls(cls)

    @classmethod
    def is_matching_object(cls, obj):
        return cls.is_matching_cls(type(obj))

    @classmethod
    def create(cls, user_cls, args, kwargs, options):
        DataClassVariable._patch_once()

        skip_code(user_cls.__init__.__code__)
        keys = [f.name for f in dataclasses.fields(user_cls)]
        bound = inspect.signature(user_cls).bind(*args, **kwargs)
        bound.apply_defaults()
        assert set(bound.arguments.keys()) == set(keys)
        items = collections.OrderedDict()
        for key in keys:
            val = bound.arguments[key]
            key = ConstantVariable.create(key)
            if isinstance(val, VariableTracker):
                items[key] = val
            else:
                if cls.include_none:
                    assert variables.ConstantVariable.is_literal(val)
                    items[key] = variables.ConstantVariable.create(val)
                else:
                    assert val is None, f"unexpected {val}"

        if len(items) == 1 and not isinstance(items[keys[0]], variables.TensorVariable):
            unimplemented("DataClassVariable iterator constructor")
            # TODO(jansel): implement unpacking logic in ModelOutput.__post_init__

        return cls(items, user_cls, **options)

    @classmethod
    def wrap(cls, builder, obj):
        user_cls = type(obj)
        keys = [f.name for f in dataclasses.fields(user_cls)]

        excluded = []
        items = collections.OrderedDict()
        for key in keys:
            # __init__ function of a dataclass might not have yet defined the key
            if hasattr(obj, key):
                val = getattr(obj, key)
                var = builder.__class__(
                    tx=builder.tx, source=AttrSource(builder.source, key)
                )(val)
                if val is not None or cls.include_none:
                    key = ConstantVariable.create(key)
                    items[key] = var
                else:
                    excluded.append(var)
        return cls(
            items, user_cls, **VariableTracker.propagate(excluded, items.values())
        )

    def __init__(self, items, user_cls, **options):
        super().__init__(items, user_cls, **options)
        assert self.is_matching_cls(user_cls)

    def as_proxy(self):
        raise NotImplementedError()

    def reconstruct(self, codegen):
        codegen.extend_output([codegen._create_load_const(self.user_cls)])
        # All the keys are just wrapped strings
        d = self.keys_as_python_constant()
        codegen.foreach(d.values())
        keys = tuple(d.keys())
        return codegen.create_call_function_kw(len(keys), keys, True)

    def call_method(
        self,
        tx,
        name,
        args: "List[VariableTracker]",
        kwargs: "Dict[str, VariableTracker]",
    ) -> "VariableTracker":
        options = VariableTracker.propagate(self, args, kwargs.values())
        if name == "__getitem__":
            assert not kwargs and len(args) == 1
            val = args[0]
            if val.python_type() == str:
                return self.getitem_const(val)
            else:
                return (
                    self.call_method(tx, "to_tuple", [], {})
                    .call_method(tx, "__getitem__", args, kwargs)
                    .add_options(options)
                )
        elif name == "to_tuple":
            assert not (args or kwargs)
            return variables.TupleVariable(list(self.items.values()), **options)
        elif name == "__setattr__":
            name = "__setitem__"
        return super().call_method(tx, name, args, kwargs)

    def var_getattr(self, tx, name: str) -> "VariableTracker":
        if name in self:
            return self.call_method(
                tx, "__getitem__", [ConstantVariable.create(name)], {}
            )
        elif not self.include_none:
            defaults = {f.name: f.default for f in dataclasses.fields(self.user_cls)}
            if name in defaults:
                assert variables.ConstantVariable.is_literal(defaults[name])
                return variables.ConstantVariable.create(defaults[name]).add_options(
                    self
                )
        super().var_getattr(tx, name)


class CustomizedDictVariable(ConstDictVariable):
    @staticmethod
    def is_matching_cls(cls):
        # True if using default OrderedDict.__init__ and did not implement __post_init__
        if (
            issubclass(cls, collections.OrderedDict)
            and cls.__init__ is collections.OrderedDict.__init__
            and not hasattr(cls, "__post_init__")
        ):
            return True
        # hack for HF usecase:
        #   assume dataclass annotation for ModelOutput subclass
        #   assume self.create is AA to ModelOutput.__post_init__
        # for non-HF usecase:
        #   check __module__ string to avoid costy HF import
        return _is_matching_transformers_cls(cls) or _is_matching_diffusers_cls(cls)

    @classmethod
    def is_matching_object(cls, obj):
        return cls.is_matching_cls(type(obj))

    # called from user_defined.py
    # when is_matching_cls(cls) is true
    @classmethod
    def create(cls, user_cls, args, kwargs, options):
        # avoid tracing when returning ModelOutput from forward func
        for attr_name in ("__init__", "__post_init__", "__setattr__", "__setitem__"):
            if hasattr(user_cls, attr_name):
                fn = getattr(user_cls, attr_name)
                assert callable(fn), f"expect callable attr {attr_name}"
                if hasattr(fn, "__code__"):
                    skip_code(fn.__code__)

        def dict_to_vars(d):
            if not all(ConstantVariable.is_literal(val) for val in d.values()):
                unimplemented("expect defaults to be literals")

            # The keys are strings
            make_const = ConstantVariable.create
            return {make_const(k): make_const(v) for k, v in d.items()}

        if dataclasses.is_dataclass(user_cls):
            # @dataclass CustomDict(a=1, b=2)
            bound = inspect.signature(user_cls).bind(*args, **kwargs)
            bound.apply_defaults()
            items = dict_to_vars(bound.arguments)
        elif not args:
            # CustomDict(a=1, b=2) in the general (non-dataclass) case.
            items = dict_to_vars(kwargs or {})
        elif len(args) == 1 and isinstance(args[0], ConstDictVariable) and not kwargs:
            # CustomDict({'a': 1, 'b': 2})
            items = args[0].items
        else:
            unimplemented("custom dict init with args/kwargs unimplemented")

        return cls(items, user_cls, **options)

    # called from builder.py
    @classmethod
    def wrap(cls, builder, obj):
        raise NotImplementedError()

    def __init__(self, items, user_cls, **options):
        super().__init__(items, user_cls, **options)
        assert self.is_matching_cls(user_cls)

    def as_proxy(self):
        raise NotImplementedError()

    # 'RETURN_VALUE triggered compile'
    # called from torch/_dynamo/codegen.py
    def reconstruct(self, codegen):
        codegen.extend_output([codegen._create_load_const(self.user_cls)])
        # All the keys are just wrapped strings
        d = self.keys_as_python_constant()
        codegen.foreach(d.values())
        keys = tuple(d.keys())
        return codegen.create_call_function_kw(len(keys), keys, True)

    def call_method(
        self,
        tx,
        name,
        args: "List[VariableTracker]",
        kwargs: "Dict[str, VariableTracker]",
    ) -> "VariableTracker":
        options = VariableTracker.propagate(self, args, kwargs.values())
        fn = getattr(self.user_cls, name)
        source = None if self.source is None else AttrSource(self.source, name)

        if hasattr(fn, "__objclass__") and fn.__objclass__ in (
            dict,
            collections.OrderedDict,
        ):
            # for python dict method without overridden
            return super().call_method(tx, name, args, kwargs)
        elif name in ("__getitem__", "to_tuple", "__setitem__", "__setattr__"):
            # for user overridden method
            return tx.inline_user_function_return(
                variables.UserFunctionVariable(fn, source=source, **options),
                [self] + list(args),
                kwargs,
            )

        unimplemented("custom dict: call_method unimplemented name=%s", name)

    def var_getattr(self, tx, name: str) -> "VariableTracker":
        if name in self:
            return self.call_method(
                tx, "__getitem__", [ConstantVariable.create(name)], {}
            )
        super().var_getattr(tx, name)


class HFPretrainedConfigVariable(VariableTracker):
    """
    Hack for HuggingFace PretrainedConfig
    """

    @staticmethod
    def is_matching_cls(cls):
        try:
            from transformers.configuration_utils import PretrainedConfig

            return issubclass(cls, PretrainedConfig)
        except ImportError:
            return False

    @classmethod
    def is_matching_object(cls, obj):
        return cls.is_matching_cls(type(obj))

    def __init__(self, obj, **kwargs):
        super().__init__(**kwargs)
        self.obj = obj
        assert self.is_matching_cls(type(obj))

    def var_getattr(self, tx, name: str) -> "VariableTracker":
        from . import ConstantVariable

        return ConstantVariable.create(getattr(self.obj, name))

    def call_hasattr(self, tx, name: str) -> "VariableTracker":
        return variables.ConstantVariable.create(hasattr(self.obj, name)).add_options(
            self
        )


class PythonSysModulesVariable(VariableTracker):
    """Special case for sys.modules.

    Without this we will guard on the exact set of modules imported in the
    lifetime of the python program.
    """

    def python_type(self):
        return dict

    @staticmethod
    def reconstruct(self, codegen):
        codegen.extend_output(
            [
                codegen.create_load_python_module(sys, True),
                codegen.create_load_attr("modules"),
            ]
        )

    def call_method(
        self, tx, name, args: List[VariableTracker], kwargs: Dict[str, VariableTracker]
    ):
        from .builder import VariableBuilder

        if name == "__getitem__":
            return self.call_getitem(tx, *args, **kwargs)
        elif name == "get":
            return self.call_get(tx, *args, **kwargs)
        elif name == "__contains__":
            return self.call_contains(tx, *args, **kwargs)

        # Fallback to dict implementation
        options = VariableTracker.propagate(self, args, kwargs.values())
        real_dict = VariableBuilder(tx, self.source, **options)(sys.modules)
        return real_dict.call_method(tx, name, args, kwargs)

    def _contains_helper(self, tx, key: VariableTracker):
        k = key.as_python_constant()
        has_key = k in sys.modules
        guard = self.make_guard(
            functools.partial(GuardBuilder.DICT_CONTAINS, key=k, invert=not has_key)
        )
        guards = {*self.guards, guard}
        return k, has_key, guards

    def call_contains(self, tx, key: VariableTracker):
        k, has_key, guards = self._contains_helper(tx, key)
        return ConstantVariable.create(
            value=has_key,
            guards=guards,
        )

    def call_get(
        self, tx, key: VariableTracker, default: Optional[VariableTracker] = None
    ):
        from .builder import VariableBuilder

        k, has_key, guards = self._contains_helper(tx, key)

        if has_key:
            return VariableBuilder(
                tx,
                GetItemSource(self.source, k),
            )(
                sys.modules[k]
            ).add_guards(guards)

        if default is not None:
            return default.add_guards(guards)

        return ConstantVariable.create(value=None, guards=guards)

    def call_getitem(self, tx, key: VariableTracker):
        from .builder import VariableBuilder

        k, has_key, guards = self._contains_helper(tx, key)
        return VariableBuilder(
            tx,
            GetItemSource(self.source, k),
        )(
            sys.modules[k]
        ).add_guards(guards)
