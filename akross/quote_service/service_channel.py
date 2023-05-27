import aio_pika
import logging

from akross.common.enums import PredefinedExchange
from akross.connection.aio.base_channel import BaseChannel


LOGGER = logging.getLogger(__name__)


class ServiceChannel(BaseChannel):
    def __init__(
        self,
        market_name: str,
        url: str,
        user: str,
        password: str,
        vhost: str,
    ):
        super().__init__(
            PredefinedExchange.Akrossquote,
            market_name,
            url,
            user,
            password,
            vhost
        )
        self._exchanges = {}
        self._quote_queue: aio_pika.abc.AbstractQueue = None
        self._api_queue: aio_pika.abc.AbstractQueue = None
        self._worker_queue: aio_pika.abc.AbstractQueue = None

    async def start_api_listening(
        self,
        quote_callback,
        api_callback
    ) -> str:
        LOGGER.warning('start api listening')

        self._quote_queue = await self.create_queue()
        await self._quote_queue.bind(PredefinedExchange.Akrossquote)
        await self._quote_queue.consume(quote_callback, no_ack=True)

        #  declare API queue
        self._api_queue = await self.create_queue()
        await self._api_queue.consume(api_callback, no_ack=True)
        return self._api_queue.name

    async def start_worker_listening(self, worker_callback):
        LOGGER.warning('start worker listening')

        self._worker_queue = await self.channel.declare_queue(
            name=self.market_name,
            exclusive=True,
            auto_delete=True
        )
        await self._worker_queue.consume(worker_callback, no_ack=True)

    def has_exchange(self, exchange_name) -> bool:
        return exchange_name in self._exchanges

    def remove_exchange_from_list(self, exchange_name) -> None:
        if exchange_name in self._exchanges:
            del self._exchanges[exchange_name]

    async def create_subscribe_exchange(
        self,
        exchange_name: str
    ) -> None:
        if exchange_name not in self._exchanges:
            LOGGER.info('create exchange for subscribe(%s)', exchange_name)
            await self.channel.declare_exchange(
                name=exchange_name,
                type=aio_pika.ExchangeType.FANOUT,
                auto_delete=False
            )
            self._exchanges[exchange_name] = True
        else:
            LOGGER.info('%s already is streaming', exchange_name)
