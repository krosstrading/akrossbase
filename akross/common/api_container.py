from typing import Dict, List
import asyncio

from .enums import CommandType, WorkerType
from .worker import Worker


class Api:
    def __init__(self, command: str, command_type: CommandType):
        self.command = command
        self.command_type: CommandType = command_type
        self.workers: List[Worker] = []

    def to_dict(self):
        return {'name': self.command, 'type': self.command_type}

    def add_worker(self, worker: Worker) -> None:
        self.workers.append(worker)

    def remove_worker(self, worker: Worker) -> int:
        self.workers.remove(worker)
        return len(self.workers)

    def get_workers(self, worker_type: WorkerType) -> None:
        return list(filter(
            lambda w: w.get_worker_type() == worker_type,
            self.workers)
        )

    def get_command_type(self) -> CommandType:
        return self.command_type


class WorkerWatcher:
    def __init__(self):
        pass

    async def on_worker_connected(self, worker: Worker):
        pass

    async def on_worker_disconnected(self, worker: Worker):
        pass


class ApiContainer:
    def __init__(self):
        self.api_to_workers: Dict[str, Api] = {}
        self.watchers: List[WorkerWatcher] = []

    def has_command(self, cmd):
        return cmd in self.api_to_workers

    def get_apis(self):
        apis = []
        for api in self.api_to_workers.values():
            apis.append(api.to_dict())
        return apis

    def get_workers(self, cmd: str, worker_type: WorkerType) -> List[Worker]:
        clients = []
        if cmd in self.api_to_workers:
            clients = self.api_to_workers[cmd].get_workers(worker_type)
        return clients

    def get_command_type(self, cmd) -> CommandType:
        # assume all clients has same type to same api name
        if cmd in self.api_to_workers:
            return self.api_to_workers[cmd].get_command_type()
        return None

    def add_worker(self, worker: Worker):
        for api in worker.supported_apis():
            if api.name not in self.api_to_workers:
                self.api_to_workers[api.name] = Api(api.name, api.command_type)
            self.api_to_workers[api.name].add_worker(worker)

        for watcher in self.watchers:
            asyncio.create_task(watcher.on_worker_connected(worker))

    def remove_worker(self, worker: Worker):
        remove_apis = []
        for key, api in self.api_to_workers.items():
            if api.remove_worker(worker) == 0:
                remove_apis.append(key)

        for api_name in remove_apis:
            del self.api_to_workers[api_name]

        for watcher in self.watchers:
            asyncio.create_task(watcher.on_worker_disconnected(worker))

    def add_watcher(self, watcher: WorkerWatcher):
        self.watchers.append(watcher)
