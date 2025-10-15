from functools import wraps

from asgiref.sync import iscoroutinefunction

_COROUTINE_FUNCTION_CACHE = {}


def xframe_options_deny(view_func):
    """
    Modify a view function so its response has the X-Frame-Options HTTP
    header set to 'DENY' as long as the response doesn't already have that
    header set. Usage:

    @xframe_options_deny
    def some_view(request):
        ...
    """

    if iscoroutinefunction(view_func):

        async def _view_wrapper(*args, **kwargs):
            response = await view_func(*args, **kwargs)
            if response.get("X-Frame-Options") is None:
                response["X-Frame-Options"] = "DENY"
            return response

    else:

        def _view_wrapper(*args, **kwargs):
            response = view_func(*args, **kwargs)
            if response.get("X-Frame-Options") is None:
                response["X-Frame-Options"] = "DENY"
            return response

    return wraps(view_func)(_view_wrapper)


def xframe_options_sameorigin(view_func):
    """
    Modify a view function so its response has the X-Frame-Options HTTP
    header set to 'SAMEORIGIN' as long as the response doesn't already have
    that header set. Usage:

    @xframe_options_sameorigin
    def some_view(request):
        ...
    """

    if iscoroutinefunction(view_func):

        async def _view_wrapper(*args, **kwargs):
            response = await view_func(*args, **kwargs)
            if response.get("X-Frame-Options") is None:
                response["X-Frame-Options"] = "SAMEORIGIN"
            return response

    else:

        def _view_wrapper(*args, **kwargs):
            response = view_func(*args, **kwargs)
            if response.get("X-Frame-Options") is None:
                response["X-Frame-Options"] = "SAMEORIGIN"
            return response

    return wraps(view_func)(_view_wrapper)


def xframe_options_exempt(view_func):
    """
    Modify a view function by setting a response variable that instructs
    XFrameOptionsMiddleware to NOT set the X-Frame-Options HTTP header. Usage:

    @xframe_options_exempt
    def some_view(request):
        ...
    """
    # Use id(view_func) for cache key to handle reloading edge cases
    func_id = id(view_func)
    try:
        is_coroutine = _COROUTINE_FUNCTION_CACHE[func_id]
    except KeyError:
        is_coroutine = iscoroutinefunction(view_func)
        _COROUTINE_FUNCTION_CACHE[func_id] = is_coroutine

    if is_coroutine:

        async def _view_wrapper(*args, **kwargs):
            response = await view_func(*args, **kwargs)
            response.xframe_options_exempt = True
            return response

    else:

        def _view_wrapper(*args, **kwargs):
            response = view_func(*args, **kwargs)
            response.xframe_options_exempt = True
            return response

    return wraps(view_func)(_view_wrapper)
