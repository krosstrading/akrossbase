from PyQt5.QtCore import QCoreApplication

from akross.connection.pika_qt.quote_channel import PqConnection
from akross.connection.pika_qt.rpc_handler import PqRpcHandler


class MockQtRpcHandler(PqRpcHandler):
    def __init__(self, conn: PqConnection):
        super().__init__()
        self._conn = conn
        self.minute = self.on_minute
        self.priceStream = self.on_price_stream

    def on_minute(self, **kwargs):
        print('on_candle')
        return [1, 2, 3, 4, 5]

    def on_price_stream(self, **kwargs):
        print('on_rprice')
        self._conn.add_subscribe_count(1)


if __name__ == '__main__':
    import sys
    import logging
    import signal
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    app = QCoreApplication([])

    signal.signal(signal.SIGINT, signal.SIG_DFL)

    conn = PqConnection('binance.spot', '127.0.0.1', 'akross', 'Akross@q')
    conn.set_capacity(100)
    conn.connect()
    mock_worker = MockQtRpcHandler(conn)
    conn.run_bus(mock_worker)
    sys.exit(app.exec_())
