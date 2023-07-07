from typing import List

from .enums import WorkerType
from .aktime import get_msec


class SubscribeObject:
    def __init__(self, command: str, target: str, exchange: str, args: dict):
        self.command = command
        self.target = target
        self.exchange = exchange
        self.args = args
        self.args['exchange'] = exchange


class WorkerApi:
    def __init__(self, name, command_type) -> None:
        self.name = name
        self.command_type = command_type


class Worker:
    TimeoutMsec = 180000  # 3 minutes

    def __init__(self, **kwargs):
        self.uuid = kwargs['uuid']
        self.apis: List[WorkerApi] = []
        for api in kwargs['api']:
            self.apis.append(WorkerApi(api['name'], api['type']))

        self.worker_type = kwargs['workerType']
        self.subscribe_count = kwargs['subscribe_count']
        self.capacity = kwargs['capacity']
        self.subscribe_objs: List[SubscribeObject] = []
        self.time = get_msec()
        self.test_id = ''

    def get_worker_type(self) -> WorkerType:
        return self.worker_type

    def supported_apis(self) -> List[WorkerApi]:
        return self.apis

    def available_subscribe_count(self) -> int:
        count = self.capacity - self.subscribe_count
        count = 0 if count < 0 else count
        return count

    def add_subscribe(self, obj: SubscribeObject) -> None:
        self.subscribe_objs.append(obj)

    def set_subscribe_count(self, count: int) -> None:
        self.subscribe_count = count

    def has_api(self, cmd) -> bool:
        for worker_api in self.apis:
            if worker_api.name == cmd:
                return True
        return False

    def pong(self) -> None:
        self.time = get_msec()

    def is_timeout(self) -> bool:
        return get_msec() - self.time > self.TimeoutMsec

    # for backtest
    def set_test_id(self, test_id: str):
        self.test_id = test_id

    def get_test_id(self):
        return self.test_id

    def __str__(self):
        apis = [api.name for api in self.apis]
        return f'uuid: {self.uuid}, type: {self.worker_type}, apis: {apis}' \
               f'subscribe: {self.subscribe_count}, capacity: {self.capacity}'

    def __repr__(self):
        return self.__str__()
