import asyncio
from typing import Any, Callable, Dict, Tuple
import aio_pika
import sys
import logging

from akross.common.enums import WorkerSuffix, WorkerType
from akross.common.util import parseResponseMsg


LOGGER = logging.getLogger(__name__)


class BaseChannel:
    def __init__(
        self,
        broadcast_exchange_name: str,
        market_name: str,
        url: str,
        user: str,
        password: str,
        vhost: str
    ):
        self.market_name = market_name
        self._url = url
        self._user = user
        self._password = password
        self._vhost = vhost
        self._msg_dict: Dict[str, asyncio.Future] = {}

        self._broadcast_exchange_name = broadcast_exchange_name
        self._connection: aio_pika.abc.AbstractConnection = None
        self._channel: aio_pika.abc.AbstractChannel = None
        self._broadcast_exchange: aio_pika.abc.AbstractExchange = None
        self._response_queue: aio_pika.abc.AbstractQueue = None

    @property
    def channel(self):
        return self._channel

    def get_subscribe_name(self, command: str, target: str) -> str:
        return self.market_name + '.' + command.lower() + '.' + target.lower()

    async def connect(self):
        LOGGER.warning('connect to %s', self._url)
        try:
            self._connection = await aio_pika.connect(
                host=self._url,
                login=self._user,
                password=self._password,
                virtualhost=self._vhost
            )
        except Exception as e:
            LOGGER.error('failed to connect %s', str(e))
            sys.exit(1)
        self._channel = await self._connection.channel()
        self._broadcast_exchange = await self._channel.declare_exchange(
            name=self._broadcast_exchange_name,
            type=aio_pika.ExchangeType.FANOUT,
            auto_delete=False)
        # to get response msg when send block msg
        self._response_queue = await self.create_queue()
        await self._response_queue.consume(self._on_response_msg, no_ack=True)
        LOGGER.warning('connected')

    def get_queue_name(self, worker_type: WorkerType) -> str:
        if worker_type == WorkerType.Cache:
            return self.market_name + WorkerSuffix.Cache
        elif worker_type == WorkerType.Online:
            return self.market_name + WorkerSuffix.Broker
        return ''

    async def _check_timeout(self, msg_id: str, secs: int):
        await asyncio.sleep(secs)
        if msg_id in self._msg_dict:
            LOGGER.error('msg timeout')
            self._msg_dict[msg_id].set_result(None)

    async def _on_response_msg(self, msg: aio_pika.abc.AbstractIncomingMessage):
        try:
            if msg.correlation_id and len(msg.correlation_id) > 0:
                if msg.correlation_id in self._msg_dict:
                    self._msg_dict[msg.correlation_id].set_result(msg)
                else:
                    LOGGER.warning('cannot find correaltion id: %s, %s',
                                   msg.correlation_id, msg.body)
            else:
                LOGGER.error('cannot find correlation ID')
        except Exception as e:
            LOGGER.error('response msg error %s', str(e))

    async def block_request(
        self,
        msg_id: str,
        queue_name: str,
        body: bytes,
        timeout: int = 10
    ) -> Tuple[str, Any]:
        await self._channel.default_exchange.publish(
            aio_pika.Message(
                body=body,
                correlation_id=msg_id,
                reply_to=self._response_queue.name
            ), routing_key=queue_name
        )

        self._msg_dict[msg_id] = asyncio.Future()
        asyncio.create_task(self._check_timeout(msg_id, timeout))
        resp = await self._msg_dict[msg_id]
        del self._msg_dict[msg_id]
        if resp is not None:
            command, payload = parseResponseMsg(resp)
            return command, payload
        return '', None

    async def publish(
        self,
        queue_name: str,
        body: bytes,
        correlation_id: str = ''
    ) -> None:
        await self._channel.default_exchange.publish(
            aio_pika.Message(
                body=body,
                correlation_id=correlation_id
            ), routing_key=queue_name
        )

    async def publish_msg(
        self,
        queue_name: str,
        msg: aio_pika.Message
    ) -> None:
        await self._channel.default_exchange.publish(msg, routing_key=queue_name)

    async def publish_broadcast_msg(self, msg: aio_pika.Message):
        await self._broadcast_exchange.publish(msg, routing_key='')

    async def create_queue(
        self,
        name: str = '',
        exclusive: bool = True,
        auto_delete: bool = True,
        callback: Callable[[aio_pika.abc.AbstractIncomingMessage], Any] = None,
        no_ack=True
    ) -> aio_pika.abc.AbstractQueue:
        queue = await self._channel.declare_queue(
            name=name,
            exclusive=exclusive,
            auto_delete=auto_delete
        )
        if callback is not None:
            await queue.consume(callback, no_ack=no_ack)
        return queue
