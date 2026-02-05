class BaseException(Exception):
    """Base exception for the application."""
    pass


class TagNotFoundError(BaseException):
    """Exception raised when a tag is not found in the soup."""
    def __init__(self, tag: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.tag: str = tag


class ParsingError(BaseException):
    """Exception raised when there is an error in parsing."""
    pass
