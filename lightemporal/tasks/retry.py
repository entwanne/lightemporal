from dataclasses import dataclass


@dataclass
class RetryPolicy:
    error_type: type[Exception] | tuple[type[Exception], ...]
    max_retries: int
    delay: int = 0
    backoff: int = 1


DEFAULT_POLICY = RetryPolicy(Exception, 10)
