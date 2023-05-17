import functools
import logging
import traceback
from typing import Any, Callable, List, Tuple
import aio_pika
import asyncio
import uuid

from akross.common import env
from akross.common.enums import Command, PredefinedExchange
from akross.common.exception import MethodError
from akross.common.util import (
    encodeRequestMsg,
    encodeResponseError,
    encodeResponseMsg,
    parseRequestMsg,
    parseResponseMsg
)

from akross.connection.aio.base_channel import BaseChannel
from akross.rpc.base import RpcBase


LOGGER = logging.getLogger(__name__)


class Account:
    def __init__(
        self,
        uuid: str,
        market: str,
        account_number: str,
        broker: str,
        base_asset: str
    ):
        self.uuid = uuid
        self.market = market
        self.account_number = account_number
        self.broker = broker
        self.base_asset = base_asset


class AccountChannel(BaseChannel):
    def __init__(
        self,
        market_name: str,
        url: str = env.get_rmq_url(),
        user: str = env.get_rmq_user(),
        password: str = env.get_rmq_password(),
        vhost: str = '/'
    ):
        super().__init__(
            PredefinedExchange.Akrossaccount,
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
        self._accounts: List[Account] = []
        self._account_exchange: aio_pika.abc.AbstractExchange = None

    @property
    def private_queue_name(self):
        return self._private_queue.name

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
        cmd, payload = parseResponseMsg(msg)
        if payload is not None:
            await callback(cmd, payload)

    async def subscribe_account(
        self,
        account: Account,
        callback: Callable[[str, Any], None]
    ) -> bool:
        queue = await self.create_queue(exclusive=True, auto_delete=True)
        await queue.consume(functools.partial(self.on_stream_msg, callback), no_ack=True)
        await queue.bind(account.market + '.' + account.account_number)

    async def api_call(
        self,
        account: Account,
        command: str,
        **kwargs
    ) -> Tuple[str, Any]:
        msg_id = str(uuid.uuid4())
        LOGGER.info('api_call(%s) account: %s, msg_id: %s, cmd: %s, args: %s',
                    account.market, account.account_number, msg_id, command, kwargs)

        ret, data = await self.block_request(
            msg_id,
            account.uuid,
            encodeRequestMsg(command, **kwargs)
        )
        LOGGER.info('get response id: %s, ret: %s)', msg_id, ret)
        return ret, data

    async def wait_for_market(self, market_name: str) -> None:
        while True:
            LOGGER.warning('wait_for_market %s', market_name)
            for account in self._accounts:
                if account.market == market_name:
                    return
            await asyncio.sleep(1)

    def get_accounts(self, market_name: str, account_number: str = '') -> List[Account]:
        accounts = []
        for account in self._accounts:
            if account.market == market_name:
                if len(account_number) > 0:
                    if account_number == account.account_number:
                        accounts.append(account)
                else:
                    accounts.append(account)
        return accounts

    async def on_broadcast_response(self, msg: aio_pika.abc.AbstractIncomingMessage):
        command, data = parseResponseMsg(msg)
        print(command, data)
        if command == Command.Discovery:
            self._accounts.append(
                Account(data['uuid'], data['market'], data['account'], data['broker'], data['baseAsset']))

    async def account_discovery(self):
        if self._recv_queue is None:
            self._recv_queue = await self.create_queue(
                callback=self.on_broadcast_response, no_ack=True)
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

    async def on_broadcast_msg(self, msg: aio_pika.abc.AbstractIncomingMessage):
        if self._bus_handler is not None:
            try:
                cmd, payload = parseRequestMsg(msg)
                LOGGER.info('%s(%s)', cmd, payload)
                if self._bus_handler.has_method(cmd) and 'seq' in payload:
                    resp = await self._bus_handler.handle(cmd)
                    resp['uuid'] = self.private_queue_name
                    resp['seq'] = payload['seq']
                    await self.publish(
                        msg.reply_to,
                        encodeResponseMsg(cmd, resp),
                        msg.correlation_id
                    )
            except Exception as e:
                LOGGER.error(str(e))

    async def run(
        self,
        account_name: str,
        handler: RpcBase
    ):
        self._bus_handler = handler
        queue = await self.create_queue(callback=self.on_broadcast_msg, no_ack=True)
        await queue.bind(PredefinedExchange.Akrossaccount)
        self._account_exchange = self.get_stream_exchange(self.market_name + '.' + account_name)

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

    async def send_event(
        self,
        cmd: str,
        payload: Any
    ) -> None:
        if self._account_exchange is not None:
            await self._account_exchange.publish(
                aio_pika.Message(
                    body=encodeResponseMsg(
                        cmd,
                        payload
                    )
                ),
                routing_key=''
            )
