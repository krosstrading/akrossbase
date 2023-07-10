import sys
import logging
import json
import pika
from pika.channel import Channel
from PyQt5.QtCore import QEventLoop

from akross.connection.pika_qt.pikaqt import PikaQtLoop
from akross.common import env


LOGGER = logging.getLogger(__name__)


class AccountSubscribeChannel:
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

        self._setup_loop = QEventLoop()

        self._connection: pika.SelectConnection = None
        self._channel: Channel = None

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

    def on_channel_open(self, channel: Channel):
        self._channel = channel
        self._setup_loop.quit()

    def wait_for_connected(self):
        if self._channel is None:
            self._setup_loop.exec()

    def send_event(self, event):
        try:
            self._channel.basic_publish(
                exchange='',
                routing_key=self.market_name + '.' + self.account + '.internal',
                body=json.dumps(event)
            )
        except Exception as e:
            LOGGER.error(str(e))
