import asyncio
from typing import Any, Dict, Tuple
import sys
import logging
import uuid
import pika
import traceback
from pika.channel import Channel
from pika.exchange_type import ExchangeType
from PyQt5.QtCore import QEventLoop, QTimer

from akross.common.exception import MethodError
from akross.common.util import encodeRequestMsg, encodeResponseError, encodeResponseMsg
from akross.common.enums import Command, PredefinedExchange, WorkerSuffix, WorkerType
from akross.connection.pika_qt import parser
from akross.connection.pika_qt.pikaqt import PikaQtLoop
from akross.connection.pika_qt.rpc_handler import RpcHandler
from akross.common import env


LOGGER = logging.getLogger(__name__)


class BlockRequestMessage:
    def __init__(self, timeout):
        self.timer = QTimer()
        self.timer.setSingleShot(True)
        self.timer.setInterval(timeout * 1000)
        self.loop = QEventLoop()
        self.timer.timeout.connect(self._handle_timeout)
        self.response = None

    def _handle_timeout(self):
        self.response = None
        LOGGER.warning('handle timeout')
        self.loop.quit()

    def set_result(self, result: bytes):
        if self.timer.isActive():
            self.timer.stop()
        self.response = result
        self.quit()

    def get_result(self):
        return self.response

    def quit(self):
        self.loop.quit()

    def block(self):
        self.timer.start()
        self.loop.exec()


class QuoteChannel:
    """
    PqConnection only supports online worker for convenience
    """
    def __init__(
        self,
        market_name: str,
        url: str = env.get_rmq_url(),
        user: str = env.get_rmq_user(),
        password: str = env.get_rmq_password(),
        vhost: str = '/'
    ):
        self.market_name = market_name
        self._url = url
        self._user = user
        self._password = password
        self._vhost = vhost
        self._msg_dict: Dict[str, asyncio.Future] = {}

        self._connection: pika.SelectConnection = None
        self._channel: Channel = None
        self._private_queue: str = ''
        self._response_queue: str = ''
        self._bus_handler: RpcHandler = None
        self._capacity = 0
        self._subscribe_count = 0
        self._setup_loop = QEventLoop()
        self._msg_dict: Dict[str, BlockRequestMessage] = {}
        self._ping_timer = QTimer()
        self._ping_timer.setInterval(20 * 1000)
        self._ping_timer.timeout.connect(self._send_ping)

    def set_capacity(self, cap: int):
        self._capacity = cap

    @property
    def capacity(self):
        return self._capacity

    def add_subscribe_count(self, count: int):
        self._subscribe_count += count
        LOGGER.warning('subscribe status %d/%d',
                       self._subscribe_count, self._capacity)

    @property
    def channel(self):
        return self._channel

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

    def on_channel_open(self, channel):
        self._channel = channel
        self._channel.exchange_declare(
            PredefinedExchange.Akrossquote,
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
            callback=self.on_response_queue_declareok
        )

    def on_response_queue_declareok(self, method_frame):
        self._response_queue = method_frame.method.queue
        self._channel.basic_consume(
            queue=self._response_queue,
            on_message_callback=self.on_response,
            auto_ack=True
        )
        self._setup_loop.quit()

    def on_response(self, ch, deliver, props, body: bytes):
        try:
            if props.correlation_id and len(props.correlation_id) > 0:
                if props.correlation_id in self._msg_dict:
                    self._msg_dict[props.correlation_id].set_result(body)
                else:
                    LOGGER.warning('cannot find correaltion id: %s, %s',
                                   props.correlation_id, body)         
        except Exception as e:
            LOGGER.error('response msg error %s', str(e))

    def run_bus(self, bus_handler: RpcHandler):
        self._bus_handler = bus_handler
        if len(self._private_queue) == 0:
            self._setup_loop.exec()

        self._channel.basic_consume(
            queue=self._private_queue,
            on_message_callback=self._on_bus_msg,
            auto_ack=False
        )

        self._channel.queue_declare(
            queue=self.get_queue_name(WorkerType.Online),
            exclusive=False,
            auto_delete=False,
            callback=self.on_bus_queue_declareok
        )

    def block_request(
        self,
        msg_id,
        queue_name: str,
        body: bytes,
        timeout: int = 10
    ) -> Tuple[str, Any]:
        self._msg_dict[msg_id] = BlockRequestMessage(timeout)
        self._channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            properties=pika.spec.BasicProperties(
                                                correlation_id=msg_id,
                                                reply_to=self._response_queue),
            body=body
        )
        LOGGER.info('block msg: %s', msg_id)
        self._msg_dict[msg_id].block()
        resp = self._msg_dict[msg_id].get_result()
        del self._msg_dict[msg_id]
        if resp is not None:
            command, payload = parser.parseResponseMsg(resp)
            return command, payload
        return '', None

    def on_bus_queue_declareok(self, method_frame):
        LOGGER.info('')
        self._channel.basic_consume(
            queue=method_frame.method.queue,
            on_message_callback=self._on_bus_msg,
            auto_ack=False
        )
        self.block_request(
            str(uuid.uuid4()), self.market_name,
            encodeRequestMsg(Command.Connect, uuid=self._private_queue,
                             workerType=WorkerType.Online.value,
                             api=self._bus_handler.extract_apis(),
                             subscribe_count=self._subscribe_count,
                             capacity=self.capacity),
        )
        self._ping_timer.start()

    def get_queue_name(self, worker_type: WorkerType) -> str:
        if worker_type == WorkerType.Cache:
            return self.market_name + WorkerSuffix.Cache
        elif worker_type == WorkerType.Online:
            return self.market_name + WorkerSuffix.Broker
        return ''

    def _send_ping(self):
        print('send ping')
        LOGGER.info('')
        command, _ = self.block_request(
            str(uuid.uuid4()), self.market_name,
            encodeRequestMsg(Command.Pong.value, uuid=self._private_queue)
        )
        if len(command) == 0:
            LOGGER.error('disconnected')

    def _on_bus_msg(self, ch, deliver, props, body: bytes):
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
                else:  # subscribe msg
                    ch.basic_publish(
                        exchange='',
                        routing_key=props.reply_to,
                        properties=props,
                        body=encodeResponseMsg(Command.OK, {'subscribe_count': self._subscribe_count})
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

    def send_realtime(self, exchange_name, cmd, data):
        try:
            self._channel.basic_publish(
                exchange=exchange_name,
                routing_key='',
                body=encodeResponseMsg(cmd, data)
            )
        except Exception as e:
            LOGGER.error(str(e))
