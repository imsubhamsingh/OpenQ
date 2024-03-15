class OpenQException(Exception):
    pass


class VisibilityTimeOutExpired(OpenQException):
    """Raised when the visibility timeout for a message has expired."""

    pass


class MessageNotFoundError(OpenQException):
    pass
