from akross.common.exception import MethodError
from akross.rpc.base import RpcBase


class RpcHandler(RpcBase):
    def __init__(self):
        super().__init__()

    def handle(self, method, **kwargs):
        if method in self._request_handlers:
            return self._request_handlers[method](**kwargs)
        raise MethodError(method)
