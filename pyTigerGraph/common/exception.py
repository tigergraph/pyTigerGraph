class TigerGraphException(Exception):
    """Generic TigerGraph Exception.

    Where possible, error message and code returned by TigerGraph will be used.
    """

    def __init__(self, message, code=None):
        self.message = message
        self.code = code
