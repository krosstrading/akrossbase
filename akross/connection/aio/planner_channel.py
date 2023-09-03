from typing import Any, Callable, Tuple
import uuid
import aio_pika
import logging
import traceback
import functools

from akross.common import env
from akross.connection.aio.base_channel import BaseChannel
from akross.common.enums import Command, PredefinedExchange
from akross.rpc.base import RpcBase
from akross.common.exception import MethodError
from akross.common.util import (
    encodeRequestMsg,
    encodeResponseError,
    encodeResponseMsg,
    parseRequestMsg,
    parseResponseMsg
)


LOGGER = logging.getLogger(__name__)


class PlannerChannel(BaseChannel):
    def __init__(
        self,
        url: str = env.get_rmq_url(),
        user: str = env.get_rmq_user(),
        password: str = env.get_rmq_password(),
        vhost: str = '/'
    ):
        super().__init__(
            PredefinedExchange.Akrossplanner,
            'global',
            url,
            user,
            password,
            vhost
        )
        self._bus_handler: RpcBase = None
        # for receiving broadcast message
        self._recv_queue: aio_pika.abc.AbstractQueue = None
        self._use_bus_auto_ack = False

        self._private_queue: aio_pika.abc.AbstractQueue = None

    async def connect(self, private_no_ack: bool = False):
        await super().connect()
        self._use_bus_auto_ack = private_no_ack

    async def on_stream_msg(self, callback, msg: aio_pika.abc.AbstractIncomingMessage):
        cmd, payload = parseResponseMsg(msg)
        if payload is not None:
            await callback(cmd, payload)

    async def subscribe_planner(
        self,
        callback: Callable[[str, Any], None]
    ) -> bool:
        queue = await self.create_queue(exclusive=True, auto_delete=True)
        await queue.consume(functools.partial(self.on_stream_msg, callback), no_ack=True)
        await queue.bind(PredefinedExchange.Akrossplanner)

    async def api_call(
        self,
        command: str,
        **kwargs
    ) -> Tuple[str, Any]:
        msg_id = str(uuid.uuid4())
        LOGGER.info('planner api_call msg_id: %s, cmd: %s, args: %s',
                    msg_id, command, kwargs)
        timeout = 60
        if 'timeout' in kwargs:
            timeout = kwargs['timeout']
        ret, data = await self.block_request(
            msg_id,
            PredefinedExchange.Akrossplanner + '.queue',
            encodeRequestMsg(command, **kwargs),
            timeout
        )
        LOGGER.info('get response id: %s, ret: %s)', msg_id, ret)
        return ret, data

    async def run_with_bus_queue(
        self,
        handler: RpcBase
    ):
        self._bus_handler = handler
        await self.create_queue(
            PredefinedExchange.Akrossplanner + '.queue', exclusive=True,
            auto_delete=True, callback=self.on_bus_msg, no_ack=False)
        
    async def on_bus_msg(self, msg: aio_pika.abc.AbstractIncomingMessage):
        if self._bus_handler is not None:
            try:
                command, payload = parseRequestMsg(msg)
                LOGGER.info('%s(%s)', command, payload)
                resp = await self._bus_handler.handle(command, **payload)
                LOGGER.info('send response %s', command)
                if resp is not None:
                    await self.publish(
                        msg.reply_to,
                        encodeResponseMsg(Command.OK, resp),
                        msg.correlation_id
                    )
            except MethodError:
                if not self._use_bus_auto_ack:
                    await msg.nack()
                return
            except Exception as e:
                LOGGER.error('cannot handle message(%s) %s', str(e), str(msg.body))
                print(traceback.format_exc())
                await self.publish(
                        msg.reply_to,
                        encodeResponseError('cannot handle msg'),
                        msg.correlation_id
                    )

            if not self._use_bus_auto_ack:
                await msg.ack()
        else:
            LOGGER.error('no handler for %s', str(msg.body))
            if not self._use_bus_auto_ack:
                await msg.nack()

    async def send_event(
        self,
        cmd: str,
        payload: Any
    ) -> None:
        """
        broadcast server to client
        """
        await self.publish_broadcast_msg(
            aio_pika.Message(
                    body=encodeResponseMsg(
                        cmd,
                        payload
                    )
            )
        )