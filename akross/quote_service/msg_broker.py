import aio_pika
import logging

from akross.common.enums import Command, WorkerType
from akross.common import env
from akross.common.util import (
    check_required_parameters,
    encodeResponseError,
    encodeResponseMsg,
    parseRequestMsg
)
from akross.quote_service.service_channel import ServiceChannel

from akross.quote_service.api_handler import ApiHandler
from .worker_handler import WorkerHandler
from akross.common.api_container import ApiContainer


LOGGER = logging.getLogger(__name__)


class MsgBroker:

    """
    MsgHandler handles message which come from broker clients
    * get APIs from broker clients
    * create rabbitmq exchanges for each symbol
    * allocating subscribe by scheduling to broker clients
    """
    def __init__(
        self,
        market_name: str,
        url: str = env.get_rmq_url(),
        user: str = env.get_rmq_user(),
        password: str = env.get_rmq_password(),
        vhost: str = '/'
    ):
        super().__init__()
        self.market_name = market_name
        self.url = url
        self.conn = ServiceChannel(market_name, url, user, password, vhost)
        self.api_queue_name = ''
        self.api_container = ApiContainer()
        self.worker_handler = WorkerHandler(market_name, self.conn, self.api_container)
        self.api_handler = ApiHandler(market_name, self.conn, self.api_container)

    async def run(self):
        await self.conn.connect()
        self.api_queue_name = await self.conn.start_api_listening(
            self.on_quote_message,
            self.on_api_message
        )
        await self.conn.start_worker_listening(
            self.on_worker_message)

    def summary(self):
        return {
            'host': self.url,
            'path': self.api_queue_name,
            'market': self.market_name,
            'api': self.api_container.get_apis()
        }

    async def on_api_message(
        self,
        msg: aio_pika.abc.AbstractIncomingMessage
    ) -> None:
        try:
            command, kwargs = parseRequestMsg(msg)
            if command == Command.CreateBacktest:
                LOGGER.info('create backtest %s', msg.body)
                # 여러 개 backtest 운영시에는 testId 추가 필요
                check_required_parameters(kwargs,
                                          'targets', 'startTime', 'endTime',
                                          'backtest', 'timeFrame')
                worker = self.worker_handler.get_available_backtest_worker(kwargs['backtest'])
                if worker:
                    await self.conn.publish_msg(worker.uuid, msg)
                else:
                    await self.conn.publish(msg.reply_to,
                                            encodeResponseError('no available worker'),
                                            msg.correlation_id)
            elif command == Command.FinishBacktest:
                check_required_parameters(kwargs, 'backtest')
                # self.worker_handler.free_backtest_worker(kwargs['uuid'])
            elif WorkerType.Backtest.value in kwargs:
                check_required_parameters(kwargs, 'backtest')
                worker = self.worker_handler.get_backtest_worker(kwargs['backtest'])
                if worker:
                    await self.conn.publish_msg(worker.uuid, msg)
                else:
                    await self.conn.publish(msg.reply_to,
                                            encodeResponseError('cannot find backtest worker'),
                                            msg.correlation_id)
            else:
                await self.api_handler.on_api_message(msg)
        except Exception as e:
            LOGGER.error('%s', str(e))
            await self.conn.publish(msg.reply_to,
                                    encodeResponseError(str(e)),
                                    msg.correlation_id)

    async def on_worker_message(
        self,
        msg: aio_pika.abc.AbstractIncomingMessage
    ) -> None:
        await self.worker_handler.on_request_msg(msg)

    async def on_quote_message(
        self,
        msg: aio_pika.abc.AbstractIncomingMessage
    ) -> None:
        command, kwargs = parseRequestMsg(msg)
        if command == Command.Discovery:
            await self.conn.publish(
                msg.reply_to,
                encodeResponseMsg(Command.Discovery, self.summary()),
                msg.correlation_id)
        else:
            LOGGER.error('unknown quote command(%s), reply_to(%s)',
                         command, msg.reply_to)
            if len(msg.reply_to) > 0:
                await self.conn.publish(
                    msg.reply_to,
                    encodeResponseError('API not available'),
                    msg.correlation_id
                )
