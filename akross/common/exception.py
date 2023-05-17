
class AkrossException(Exception):
    """Root exception for all errors"""


class ArgumentError(AkrossException):
    def __init__(self, exception_text, *args):
        super().__init__(exception_text, *args)


class SymbolError(AkrossException):
    def __init__(self, *args):
        super().__init__("cannot find symbol", *args)


class MethodError(AkrossException):
    def __init__(self, *args):
        super().__init__("cannot find method", *args)


class CommunicationError(AkrossException):
    def __init__(self, *args):
        super().__init__("comm error", *args)


class MessageParseError(AkrossException):
    def __init__(self, *args):
        super().__init__("cannot decode msg", *args)
