from functools import WRAPPER_ASSIGNMENTS

from asgiref.sync import iscoroutinefunction


def no_append_slash(view_func):
    """
    Mark a view function as excluded from CommonMiddleware's APPEND_SLASH
    redirection.
    """
    # view_func.should_append_slash = False would also work, but decorators are
    # nicer if they don't have side effects, so return a new function.

    is_coro = iscoroutinefunction(view_func)

    if is_coro:

        async def _view_wrapper(request, *args, **kwargs):
            return await view_func(request, *args, **kwargs)

    else:

        def _view_wrapper(request, *args, **kwargs):
            return view_func(request, *args, **kwargs)

    _view_wrapper.should_append_slash = False

    for attr in WRAPPER_ASSIGNMENTS:
        try:
            setattr(_view_wrapper, attr, getattr(view_func, attr))
        except AttributeError:
            pass

    return _view_wrapper
