import functools
import inspect


@functools.lru_cache(maxsize=512)
def _get_func_parameters(func, remove_first):
    parameters = tuple(inspect.signature(func).parameters.values())
    if remove_first:
        parameters = parameters[1:]
    return parameters


def _get_callable_parameters(meth_or_func):
    # Fast path: direct function, avoid attribute check unless necessary
    is_method = inspect.ismethod(meth_or_func)
    if is_method:
        # CPython: Bound method: __func__ is usually a simple attribute access, but this check is inexpensive vs param extraction
        func = meth_or_func.__func__
        return _get_func_parameters(func, remove_first=True)
    else:
        # Typical case: already function, pass directly
        return _get_func_parameters(meth_or_func, remove_first=False)


ARG_KINDS = frozenset(
    {
        inspect.Parameter.POSITIONAL_ONLY,
        inspect.Parameter.KEYWORD_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    }
)


def get_func_args(func):
    params = _get_callable_parameters(func)
    return [param.name for param in params if param.kind in ARG_KINDS]


def get_func_full_args(func):
    """
    Return a list of (argument name, default value) tuples. If the argument
    does not have a default value, omit it in the tuple. Arguments such as
    *args and **kwargs are also included.
    """
    params = _get_callable_parameters(func)
    args = []
    for param in params:
        name = param.name
        # Ignore 'self'
        if name == "self":
            continue
        if param.kind == inspect.Parameter.VAR_POSITIONAL:
            name = "*" + name
        elif param.kind == inspect.Parameter.VAR_KEYWORD:
            name = "**" + name
        if param.default != inspect.Parameter.empty:
            args.append((name, param.default))
        else:
            args.append((name,))
    return args


def func_accepts_kwargs(func):
    """Return True if function 'func' accepts keyword arguments **kwargs."""
    return any(p for p in _get_callable_parameters(func) if p.kind == p.VAR_KEYWORD)


def func_accepts_var_args(func):
    """
    Return True if function 'func' accepts positional arguments *args.
    """
    return any(p for p in _get_callable_parameters(func) if p.kind == p.VAR_POSITIONAL)


def method_has_no_args(meth):
    """Return True if a method only accepts 'self'."""
    count = len([p for p in _get_callable_parameters(meth) if p.kind in ARG_KINDS])
    return count == 0 if inspect.ismethod(meth) else count == 1


def func_supports_parameter(func, name):
    # Use generator expression for short-circuit, but optimize by checking identity for common parameter names
    for param in _get_callable_parameters(func):
        if param.name == name:
            return True
    return False
