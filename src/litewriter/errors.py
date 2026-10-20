"""The error types. ``str(exc)`` carries context, help, and an example."""

import math


def check_seconds(
    value: object, what: str, example: str, help_text: str | None = None
) -> float:
    """``value`` as a count of seconds, or a QueryError."""
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise QueryError(f"{what} must be a number of seconds", example=example)
    try:
        seconds = float(value)
    except OverflowError:
        seconds = math.inf
    if not math.isfinite(seconds) or seconds < 0:
        raise QueryError(
            f"{what} must be 0 or greater", help_text=help_text, example=example
        )
    return seconds


class LiteError(Exception):
    """Base of every litewriter error. ``str(exc)`` includes help."""

    __slots__ = ("context", "example", "help_text", "message")

    def __init__(
        self,
        message: str,
        *,
        context: dict[str, object] | None = None,
        help_text: str | None = None,
        example: str | None = None,
    ) -> None:
        self.message = message
        self.context = dict(context) if context else {}
        self.help_text = help_text
        self.example = example
        super().__init__(message)

    def __str__(self) -> str:
        parts = [self.message]
        if self.context:
            ctx = ", ".join(f"{k}={v!r}" for k, v in self.context.items())
            parts.append(f"  Context: {ctx}")
        if self.help_text:
            parts.append(f"  Help: {self.help_text}")
        if self.example:
            parts.append(f"  Example:\n{self.example}")
        return "\n".join(parts)


class QueryError(LiteError, ValueError):
    """Bad arguments or a bad query: unknown name, missing param, no row."""


class StateError(LiteError, RuntimeError):
    """Right call, wrong time: not started, closed, or a blocked loop."""


class ClaimBusy(LiteError):
    """Another process holds the claim."""
