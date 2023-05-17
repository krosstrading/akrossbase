import logging
import aio_pika

from akross.common.api_container import ApiContainer
from akross.common.enums import Command
from akross.common.util import encodeResponseError, encodeResponseMsg, parseRequestMsg
from akross.quote_service.service_channel import ServiceChannel

from ..rpc.base import RpcBase


LOGGER = logging.getLogger(__name__)


class RpcHandler(RpcBase):
    """
    RpcHandler is for defining APIs for clients
    """
    def __init__(
        self,
        market_name: str,
        conn: ServiceChannel,
        api_container: ApiContainer
    ):
        super().__init__()
        self._market_name = market_name
        self._conn = conn
        self._api_container = api_container

    @property
    def connection(self) -> ServiceChannel:
        return self._conn

    @property
    def api_container(self) -> ApiContainer:
        return self._api_container

    @property
    def market_name(self) -> str:
        return self._market_name

    async def handle_api_message(
        self,
        msg: aio_pika.abc.AbstractIncomingMessage,
        command: str,
        **kwargs
    ):
        pass

    async def on_api_message(
        self,
        msg: aio_pika.abc.AbstractIncomingMessage
    ) -> None:
        try:
            command, kwargs = parseRequestMsg(msg)
            await self.handle_api_message(msg, command, **kwargs)
        except Exception as e:
            LOGGER.warning(str(e))
            resp = encodeResponseError(str(e))
            if msg.reply_to is not None:
                await self._conn.publish(
                    msg.reply_to,
                    resp,
                    msg.correlation_id
                )

    async def on_request_msg(
        self,
        msg: aio_pika.abc.AbstractIncomingMessage
    ) -> None:
        try:
            command, kwargs = parseRequestMsg(msg)
            result = await self.handle(command, **kwargs)
            resp = encodeResponseMsg(Command.OK, result)
        except Exception as e:
            LOGGER.warning(str(e))
            resp = encodeResponseError(str(e))

        if msg.reply_to is not None:
            await self._conn.publish(
                msg.reply_to,
                resp,
                msg.correlation_id
            )
        else:
            LOGGER.warning('skip to reply msg(no reply_to)')
