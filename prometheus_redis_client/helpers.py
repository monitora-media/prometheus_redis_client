import time
from collections.abc import Callable
from functools import wraps


def timeit(metric_callback: Callable[[int | float], dict[str, str | int]], **labels):
    """Decorator to measure time of function execution store the duration in a metric."""

    def wrapper(func):
        @wraps(func)
        def func_wrapper(*args, **kwargs):
            start = time.monotonic()
            result = func(*args, **kwargs)
            metric_callback(time.monotonic() - start, labels=labels)
            return result
        return func_wrapper
    return wrapper
