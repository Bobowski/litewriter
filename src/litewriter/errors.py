class WriterError(Exception):
    """Wrong arguments or a bad call. ``str(exc)`` includes help."""

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


class WriterRuntime(WriterError):
    """Right call, wrong time (not started, already closed)."""


class WriterRolledBack(WriterError):
    """This write was undone because a non-isolated sibling failed the batch."""
