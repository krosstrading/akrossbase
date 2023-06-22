import functools
import logging
import traceback
from typing import Any, Callable, List, Tuple
import aio_pika
import asyncio

import uuid
from akross.common import env
from akross.common.enums import Command, PredefinedExchange, WorkerType
from akross.common.exception import MethodError
from akross.common.util import (
    encodeBacktestStream,
    encodeRequestMsg,
    encodeResponseError,
    encodeResponseMsg,
    parseRequestMsg,
    parseResponseMsg
)

from akross.connection.aio.base_channel import BaseChannel
from akross.rpc.base import RpcBase


LOGGER = logging.getLogger(__name__)


class Market:
    def __init__(
        self,
        host: str,
        path: str,
        market: str,
        api: list
    ):
        self.host = host
        self.path = path
        self.market = market
        self.api = api


class QuoteChannel(BaseChannel):
    def __init__(
        self,
        market_name: str,
        url: str = env.get_rmq_url(),
        user: str = env.get_rmq_user(),
        password: str = env.get_rmq_password(),
        vhost: str = '/'
    ):
        super().__init__(
            PredefinedExchange.Akrossquote,
            market_name,
            url,
            user,
            password,
            vhost
        )
        self._bus_handler: RpcBase = None
        # for receiving broadcast message
        self._recv_queue: aio_pika.abc.AbstractQueue = None
        self._private_queue: aio_pika.abc.AbstractQueue = None
        self._use_bus_auto_ack = False
        self._seq = 0
        self._markets: List[Market] = []
        self._capacity = 0
        self._subscribe_count = 0

    @property
    def private_queue_name(self):
        return self._private_queue.name

    @property
    def capacity(self):
        return self._capacity

    @property
    def subscribe_count(self):
        return self._subscribe_count

    def set_capacity(self, cap: int):
        self._capacity = cap

    def add_subscribe_count(self, count: int):
        self._subscribe_count += count

    @property
    def seq(self):
        self._seq += 1
        return self._seq

    async def connect(self, private_no_ack: bool = False):
        await super().connect()
        self._use_bus_auto_ack = private_no_ack
        self._private_queue = await self.create_queue(
                callback=self.on_bus_msg, no_ack=private_no_ack)

    async def on_stream_msg(self, callback, msg: aio_pika.abc.AbstractIncomingMessage):
        _, payload = parseResponseMsg(msg)
        if payload is not None:
            await callback(payload)

    async def subscribe_stream(
        self,
        market: Market,
        command: str,
        callback: Callable[[Any], None],
        **kwargs
    ) -> bool:
        ret, data = await self.api_call(market, command, **kwargs)
        if data is not None and 'exchange' in data:
            LOGGER.info('listen %s', data['exchange'])
            queue = await self.create_queue(exclusive=True, auto_delete=True)
            await queue.consume(functools.partial(self.on_stream_msg, callback), no_ack=True)
            await queue.bind(data['exchange'])
            return True
        else:
            LOGGER.error('failed to subscribe %s', kwargs['target'])
        return False

    async def api_call(
        self,
        market: Market,
        command: str,
        **kwargs
    ) -> Tuple[str, Any]:
        msg_id = str(uuid.uuid4())
        LOGGER.info('api_call(%s) msg_id: %s, cmd: %s, args: %s',
                    market.path, msg_id, command, kwargs)
        timeout = 60
        if 'timeout' in kwargs:
            timeout = kwargs['timeout']
        ret, data = await self.block_request(
            msg_id,
            market.path,
            encodeRequestMsg(command, **kwargs),
            timeout
        )
        LOGGER.info('get response id: %s, ret: %s)', msg_id, ret)
        return ret, data

    async def wait_for_market(self, market_name: str) -> None:
        while True:
            LOGGER.warning('wait_for_market %s', market_name)
            for market in self._markets:
                if market.market == market_name:
                    return
            await asyncio.sleep(1)

    def get_markets(self, market_name: str) -> List[Market]:
        markets = []
        for market in self._markets:
            if market.market == market_name:
                markets.append(market)
        return markets

    async def on_broadcast_msg(self, msg: aio_pika.abc.AbstractIncomingMessage):
        command, data = parseResponseMsg(msg)
        if command == Command.Discovery:
            self._markets.append(
                Market(data['host'], data['path'], data['market'], data['api']))

    async def market_discovery(self):
        if self._recv_queue is None:
            self._recv_queue = await self.create_queue(
                callback=self.on_broadcast_msg, no_ack=True)
        await self.publish_broadcast_msg(aio_pika.Message(
            body=encodeRequestMsg(Command.Discovery, seq=self.seq),
            correlation_id=str(uuid.uuid4()),
            reply_to=self._recv_queue.name
        ))

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
                else:  # subscribe message
                    await self.publish(
                        msg.reply_to,
                        encodeResponseMsg(Command.OK, {'subscribe_count': self._subscribe_count}),
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

    async def send_ping(self):
        while True:
            await asyncio.sleep(20)
            LOGGER.info('')
            command, _ = await self.request_to_quote(
                encodeRequestMsg(Command.Pong.value, uuid=self.private_queue_name))
            if command is None or len(command) == 0:
                LOGGER.error('disconnected')

    async def request_to_quote(self, body: bytes):
        return await self.block_request(
            str(uuid.uuid4()), self.market_name, body)

    async def run_with_bus_queue(
        self,
        worker_type: WorkerType,
        handler: RpcBase
    ):
        self._bus_handler = handler
        if worker_type != WorkerType.Backtest:
            queue_name = self.get_queue_name(worker_type)
            await self.create_queue(
                queue_name, exclusive=False,
                auto_delete=False, callback=self.on_bus_msg, no_ack=False)
        await self.request_to_quote(
            encodeRequestMsg(Command.Connect, uuid=self.private_queue_name,
                             workerType=worker_type.value, api=handler.extract_apis(),
                             subscribe_count=self.subscribe_count,
                             capacity=self.capacity)
        )
        asyncio.create_task(self.send_ping())

    def get_backtest_exchange_name(self, command: str, target: str) -> str:
        return 'backtest.' + self.market_name + '.' + command.lower() + '.' + target.lower()

    async def get_stream_exchange(
        self,
        exchange_name: str,
        auto_delete: bool = False
    ) -> aio_pika.abc.AbstractExchange:
        return await self.channel.declare_exchange(
            name=exchange_name,
            type=aio_pika.ExchangeType.FANOUT,
            auto_delete=auto_delete
        )

    async def publish_stream(
        self,
        exchange: aio_pika.abc.AbstractExchange,
        cmd: str,
        payload: Any
    ) -> None:
        await exchange.publish(
            aio_pika.Message(
                body=encodeResponseMsg(
                    cmd,
                    payload
                )
            ),
            routing_key=''
        )

    async def publish_backtest_stream(
        self,
        exchange: aio_pika.abc.AbstractExchange,
        cmd: str,
        target: str,
        payload: Any
    ) -> None:
        await exchange.publish(
            aio_pika.Message(
                body=encodeBacktestStream(
                    cmd,
                    target,
                    payload
                )
            ),
            routing_key=''
        )
