import asyncio

from akross.rpc.base import RpcBase
from akross.connection.client import ClientConnection


class MockWorkerHandler(RpcBase):
    def __init__(self, conn: ClientConnection):
        super().__init__()
        self._conn = conn
        self.candle = self.on_candle
        self.priceStream = self.on_price_stream
        self.createBacktest = self.on_create_backtest

    async def on_create_backtest(self, **kwargs):
        return {'uuid': self._conn.private_queue_name, 'exchange': 'helloworld'}

    async def on_candle(self, **kwargs):
        print('on_candle')
        return [1, 2, 3, 4, 5]

    async def on_price_stream(self, **kwargs):
        print('on_rprice')
        self._conn.add_subscribe_count(1)


async def main(worker_type):
    global my_uuid
    conn = ClientConnection('hello.spot', '127.0.0.1')
    conn.set_capacity(100)

    await conn.connect()
    mock_worker = MockWorkerHandler(conn)
    await conn.run_with_bus_queue(worker_type, mock_worker)

    await asyncio.get_running_loop().create_future()


if __name__ == '__main__':
    import sys
    import logging
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    asyncio.run(main(sys.argv[1]))
