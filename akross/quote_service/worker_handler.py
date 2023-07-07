from typing import Dict, List, Optional
import logging
import asyncio

from akross.common.api_container import ApiContainer
from akross.common.enums import WorkerType
from akross.quote_service.service_channel import ServiceChannel
from akross.quote_service.handler import RpcHandler
from akross.common.util import check_required_parameters
from akross.common.worker import Worker


LOGGER = logging.getLogger(__name__)


class WorkerHandler(RpcHandler):
    def __init__(
        self,
        market_name: str,
        conn: ServiceChannel,
        api_container: ApiContainer
    ):
        super().__init__(market_name, conn, api_container)
        self._workers: Dict[str, Worker] = {}

        self.connect = self.on_connect
        self.pong = self.on_pong
        asyncio.create_task(self.check_timeout())

    async def remove_workers(self, workers: List[Worker]):
        for worker in workers:
            LOGGER.warning('remove worker %s', worker.uuid)
            if worker.get_worker_type() != WorkerType.Backtest:
                self.api_container.remove_worker(worker)
            del self._workers[worker.uuid]

    def get_available_backtest_worker(self, test_id: str) -> Optional[Worker]:
        # 임시로 뒤에 연결된 backtest worker 할당
        for worker in reversed(self._workers.values()):
            if worker.get_worker_type() == WorkerType.Backtest:
                # if len(worker.get_test_id()) == 0:    # block temporarily for testing
                LOGGER.warning('backtest created %s', str(worker))
                worker.set_test_id(test_id)
                return worker
        return None

    def get_backtest_worker(self, test_id) -> Optional[Worker]:
        for worker in self._workers.values():
            if worker.get_test_id() == test_id:
                return worker
        return None

    async def check_timeout(self):
        while True:
            await asyncio.sleep(10)
            disconnected = []
            for worker in self._workers.values():
                if worker.is_timeout():
                    disconnected.append(worker)
            await self.remove_workers(disconnected)

    async def on_connect(self, **kwargs):
        check_required_parameters(kwargs, 'uuid', 'workerType',
                                  'api', 'subscribe_count', 'capacity')
        LOGGER.warning('new worker connected %s(type:%s) %d/%d, apis:%s',
                       kwargs['uuid'], kwargs['workerType'],
                       kwargs['subscribe_count'], kwargs['capacity'], kwargs['api'])
        uuid = kwargs['uuid']
        worker = Worker(**kwargs)
        self._workers[uuid] = worker
        if worker.get_worker_type() != WorkerType.Backtest:
            self.api_container.add_worker(self._workers[uuid])

    async def on_pong(self, **kwargs):
        check_required_parameters(kwargs, 'uuid')
        # LOGGER.info('%s %s',
        #             kwargs['uuid'],
        #             'OK' if kwargs['uuid'] in self._workers else 'no matched')
        if kwargs['uuid'] in self._workers:
            self._workers[kwargs['uuid']].pong()
