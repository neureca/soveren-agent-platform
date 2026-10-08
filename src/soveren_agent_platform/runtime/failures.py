"""Failures whose operation must not be automatically executed again."""


class NonRetryableEventError(RuntimeError):
    """The current event has a final outcome, including an exhausted inner retry budget."""


def is_non_retryable_event_error(error: BaseException) -> bool:
    if isinstance(error, NonRetryableEventError):
        return True
    if isinstance(error, BaseExceptionGroup):
        # A failed cleanup cannot make the original operation safe to repeat.
        return any(is_non_retryable_event_error(item) for item in error.exceptions)
    return False


def event_error_detail(error: BaseException) -> str:
    if isinstance(error, BaseExceptionGroup):
        return "; ".join(event_error_detail(item) for item in error.exceptions)
    return f"{type(error).__name__}: {error}"
