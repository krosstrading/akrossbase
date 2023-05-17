import asyncio
from typing import Dict
import sys
import logging
import pika
import traceback
from pika.channel import Channel
from pika.exchange_type import ExchangeType
from PyQt5.QtCore import QEventLoop

from akross.common.exception import MethodError
from akross.common.util import encodeResponseError, encodeResponseMsg
from akross.common.enums import Command, PredefinedExchange
from akross.connection.pika_qt import parser
from akross.connection.pika_qt.pikaqt import PikaQtLoop
from akross.connection.pika_qt.rpc_handler import RpcHandler
from akross.common import env


LOGGER = logging.getLogger(__name__)


class AccountChannel:
    """
    PqConnection only supports online worker for convenience
    """
    def __init__(
        self,
        market_name: str,
        account: str,
        url: str = env.get_rmq_url(),
        user: str = env.get_rmq_user(),
        password: str = env.get_rmq_password(),
        vhost: str = '/'
    ):
        self.market_name = market_name
        self.account = account
        self._url = url
        self._user = user
        self._password = password
        self._vhost = vhost
        self._msg_dict: Dict[str, asyncio.Future] = {}

        self._connection: pika.SelectConnection = None
        self._channel: Channel = None
        self._private_queue: str = ''
        self._broadcast_queue: str = ''
        self._bus_handler: RpcHandler = None
        self._setup_loop = QEventLoop()

    @property
    def channel(self):
        return self._channel
    
    @property
    def uid(self):
        return self._private_queue

    def connect(self):
        LOGGER.warning('connect to %s', self._url)
        try:
            self._connection = pika.SelectConnection(
                pika.ConnectionParameters(
                    host=self._url,
                    virtual_host=self._vhost,
                    credentials=pika.PlainCredentials(self._user, self._password)
                ),
                on_open_callback=self.on_connection_open,
                custom_ioloop=PikaQtLoop()
            )
        except Exception as e:
            LOGGER.error('failed to connect %s', str(e))
            sys.exit(1)

    def on_connection_open(self, _unused):
        self._connection.channel(on_open_callback=self.on_channel_open)
        LOGGER.warning('connected')

    def on_channel_open(self, channel: Channel):
        self._channel = channel
        self._channel.exchange_declare(
            PredefinedExchange.Akrossaccount,
            exchange_type=ExchangeType.fanout,
            auto_delete=False,
            callback=self.on_exchange_ok
        )

    def on_exchange_ok(self, method_frame):
        self._channel.queue_declare(
            queue='',
            exclusive=True,
            auto_delete=True,
            callback=self.on_private_queue_declareok
        )

    def on_private_queue_declareok(self, method_frame):
        self._private_queue = method_frame.method.queue
        self._channel.queue_declare(
            queue='',
            exclusive=True,
            auto_delete=True,
            callback=self.on_broadcast_queue_declareok
        )

    def on_broadcast_queue_declareok(self, method_frame):
        self._broadcast_queue = method_frame.method.queue
        self._channel.queue_bind(
            queue=self._broadcast_queue,
            exchange=PredefinedExchange.Akrossaccount
        )
        self._channel.exchange_declare(
            self.market_name + '.' + self.account,
            exchange_type=ExchangeType.fanout,
            auto_delete=False,
            callback=self.on_account_exchange_ok
        )

    def on_account_exchange_ok(self, method_frame):
        self._setup_loop.quit()

    def on_broadcast_msg(self, ch, deliver, props, body: bytes):
        if self._bus_handler is not None:
            try:
                cmd, payload = parser.parseRequestMsg(body)
                LOGGER.info('%s(%s)', cmd, payload)
                if self._bus_handler.has_method(cmd) and 'seq' in payload:
                    resp = self._bus_handler.handle(cmd)
                    resp['uuid'] = self._private_queue
                    resp['seq'] = payload['seq']
                    ch.basic_publish(
                        exchange='',
                        routing_key=props.reply_to,
                        properties=props,
                        body=encodeResponseMsg(Command.Discovery, resp)
                    )
            except Exception as e:
                LOGGER.error(str(e))

    def run_bus(self, bus_handler: RpcHandler):
        self._bus_handler = bus_handler
        if len(self._private_queue) == 0:
            self._setup_loop.exec()

        self._channel.basic_consume(
            queue=self._private_queue,
            on_message_callback=self._on_bus_msg,
            auto_ack=False
        )

        self._channel.basic_consume(
            queue=self._broadcast_queue,
            on_message_callback=self.on_broadcast_msg,
            auto_ack=True
        )

    def _on_bus_msg(self, ch: Channel, deliver, props, body: bytes):
        if self._bus_handler is not None:
            try:
                cmd, payload = parser.parseRequestMsg(body)
                resp = self._bus_handler.handle(cmd, **payload)
                LOGGER.info('%s(%s)', cmd, payload)
                if resp is not None:
                    ch.basic_publish(
                        exchange='',
                        routing_key=props.reply_to,
                        properties=props,
                        body=encodeResponseMsg(Command.OK, resp)
                    )
            except MethodError:
                ch.basic_nack(deliver.delivery_tag)
                return
            except Exception as e:
                LOGGER.error('cannot handle message(%s) %s', str(e), str(body))
                print(traceback.format_exc())
                ch.basic_publish(
                    exchange='',
                    routing_key=props.reply_to,
                    properties=props,
                    body=encodeResponseError('cannot handle msg')
                )
            ch.basic_ack(deliver.delivery_tag)
        else:
            LOGGER.error('no handler for %s', str(body))
            ch.basic_nack(deliver.delivery_tag)

    def send_event(self, cmd, data):
        try:
            self._channel.basic_publish(
                exchange=self.market_name + '.' + self.account,
                routing_key='',
                body=encodeResponseMsg(cmd, data)
            )
        except Exception as e:
            LOGGER.error(str(e))
