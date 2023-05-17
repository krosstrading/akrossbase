
import logging
from typing import Any, Dict, List
from akross.common.enums import CommandType

from akross.common.exception import MethodError


LOGGER = logging.getLogger(__name__)


class RpcBase:
    """
    RpcBase is for defining APIs for clients
    """
    def __init__(self):
        self._request_handlers: Dict[str, Any] = {}

    async def handle(self, method, **kwargs):
        if method in self._request_handlers:
            return await self._request_handlers[method](**kwargs)
        raise MethodError(method)

    def __register(self, method_name, callback):
        self._request_handlers[method_name] = callback

    def __setattr__(self, method_name, callback):
        if '_' in method_name:  # prevent calls for private methods
            return super().__setattr__(method_name, callback)
        return self.__register(method_name, callback)

    def is_unsubscribe(self, kwargs):
        if 'stop' in kwargs:
            return True
        return False

    def has_method(self, method):
        return method in self._request_handlers

    def extract_apis(self) -> List[dict]:
        apis = []
        for rpc in self._request_handlers.keys():
            rpc_type = CommandType.ReqRes
            if rpc.lower().endswith('stream'):
                rpc_type = CommandType.Subscribe
            apis.append({'name': rpc, 'type': rpc_type})
        return apis
