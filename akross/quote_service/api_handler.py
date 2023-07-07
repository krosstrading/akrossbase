import asyncio
from typing import List
import uuid
import aio_pika
import logging
import aiohttp
import urllib.parse

from akross.common.api_container import ApiContainer, WorkerWatcher
from akross.common.enums import Command, CommandType, WorkerType
from akross.common.worker import SubscribeObject, Worker
from akross.quote_service.service_channel import ServiceChannel
from akross.quote_service.handler import RpcHandler
from akross.common.util import encodeRequestMsg, encodeResponseError, encodeResponseMsg


LOGGER = logging.getLogger(__name__)


class ApiHandler(RpcHandler, WorkerWatcher):
    def __init__(
        self,
        url: str,
        user: str,
        password: str,
        vhost: str,
        market_name: str,
        conn: ServiceChannel,
        api_container: ApiContainer
    ):
        WorkerWatcher.__init__(self)
        RpcHandler.__init__(self, market_name, conn, api_container)
        self._url = url
        self._user = user
        self._password = password
        self._vhost = vhost
        self.api_container.add_watcher(self)
        self._subscribe_wait_q: List[SubscribeObject] = []
        self._lock = asyncio.Lock()

    async def on_worker_connected(self, worker: Worker):
        if worker.available_subscribe_count() == 0:
            return
        await self.assign_pendings()

    async def get_source_count(self, exchange_name):
        url = (f'http://{self._url}:15672/api/exchanges/'
               f'{urllib.parse.quote(self._vhost, safe="")}/'
               f'{exchange_name}/bindings/source')
        LOGGER.debug('check %s', self._url)
        async with aiohttp.ClientSession() as session:
            async with session.get(
                    url, auth=aiohttp.BasicAuth(self._user, self._password)) as response:
                if response.status == 200:
                    return len(await response.json())
                else:
                    LOGGER.warning('status code error %s', response)
        return -1

    async def assign_pendings(self):
        async with self._lock:
            assigned = []
            for obj in self._subscribe_wait_q:
                binding_count = await self.get_source_count(obj.exchange)
                LOGGER.info('bounding count for %s: %d', obj.exchange, binding_count)
                if binding_count == 0:
                    self.connection.remove_exchange_from_list(obj.exchange)
                    assigned.append(obj)  # remove from wait queue since no bindings
                else:
                    if await self.assign_subscribe(obj):
                        assigned.append(obj)
            for obj in assigned:
                self._subscribe_wait_q.remove(obj)

    async def on_worker_disconnected(self, worker: Worker):
        async with self._lock:
            self._subscribe_wait_q.extend(worker.subscribe_objs)
        await self.assign_pendings()

    async def handle_request(
        self,
        msg: aio_pika.abc.AbstractIncomingMessage,
        command: str,
        **kwargs
    ):
        online = (WorkerType.Cache.value in kwargs and
                  not kwargs[WorkerType.Cache.value])

        worker_type = WorkerType.Cache if not online else WorkerType.Online
        workers = self.api_container.get_workers(command, worker_type)

        if worker_type == WorkerType.Cache and len(workers) == 0:
            #  change to online if there is no cache worker
            worker_type = WorkerType.Online
            workers = self.api_container.get_workers(command, worker_type)

        queue_name = self.connection.get_queue_name(worker_type)
        LOGGER.info('cmd: %s, worker count %d, queue: %s',
                    command, len(workers), queue_name)
        if len(workers) > 0:
            await self.connection.publish_msg(queue_name, msg)
        else:
            LOGGER.error('no both workers and reply_to %s', msg.body)
            if len(msg.reply_to) > 0:
                await self.connection.publish(
                    msg.reply_to,
                    encodeResponseError(f'cmd({command}) no available worker'),
                    msg.correlation_id
                )

    async def assign_subscribe(self, obj: SubscribeObject):
        processed = False
        workers = self.api_container.get_workers(obj.command, WorkerType.Online)
        candidate: List[Worker] = list(
            filter(lambda w: w.available_subscribe_count() > 0, workers))
        candidate.sort(key=lambda w: w.available_subscribe_count(), reverse=True)
        LOGGER.info('%s(%s), cand(%s)', obj.command, obj.exchange, len(candidate))

        if len(candidate) > 0:
            for worker in candidate:
                result, data = await self.connection.block_request(
                    str(uuid.uuid4()),
                    worker.uuid,
                    encodeRequestMsg(obj.command, **obj.args)
                )

                if result == Command.OK and 'subscribe_count' in data:
                    processed = True
                    worker.add_subscribe(obj)
                    worker.set_subscribe_count(data['subscribe_count'])
                    break
        return processed

    async def handle_subscribe(
        self,
        msg: aio_pika.abc.AbstractIncomingMessage,
        command: str,
        **kwargs
    ):
        if 'target' in kwargs:
            exchange = self.connection.get_subscribe_name(command, kwargs['target'])
            is_new = False
            if not self.connection.has_exchange(exchange):
                is_new = True
                await self.connection.create_subscribe_exchange(exchange)

            await self.connection.publish(
                msg.reply_to,
                encodeResponseMsg(Command.OK, {'exchange': exchange}),
                msg.correlation_id
            )

            if is_new:
                subscribe_obj = SubscribeObject(command, kwargs['target'], exchange, kwargs)
                async with self._lock:
                    if not await self.assign_subscribe(subscribe_obj):
                        self._subscribe_wait_q.append(subscribe_obj)
        else:
            await self.connection.publish(
                msg.reply_to,
                encodeResponseError('no target in arguments'),
                msg.correlation_id
            )

    async def handle_api_message(
        self,
        msg: aio_pika.abc.AbstractIncomingMessage,
        command: str,
        **kwargs
    ):
        """
        venv and online supports subscribe, not cache
        """
        cmd_type = self.api_container.get_command_type(command)

        if cmd_type is None:
            if len(msg.reply_to) > 0:
                LOGGER.error('API not available cmd(%s)', command)
                await self.connection.publish(
                    msg.reply_to,
                    encodeResponseError(f'cmd({command}) not available'),
                    msg.correlation_id
                )
            else:
                LOGGER.error('no both workers and reply_to %s', msg.body)
        elif cmd_type == CommandType.ReqRes:
            await self.handle_request(msg, command, **kwargs)
        elif cmd_type == CommandType.Subscribe:
            await self.handle_subscribe(msg, command, **kwargs)
