from pika.adapters.select_connection import AbstractSelectorIOLoop
import pika.compat
# pylint: disable=E0401
from PyQt5.QtCore import (
    QSocketNotifier,
    QTimer,
    QObject,
    pyqtSignal,
    QMutexLocker,
    QMutex,
    Qt
)
import logging
import collections
import select
import threading


LOGGER = logging.getLogger(__name__)


class SocketNotifier:
    def __init__(self, parent, fd, handler, events):
        self._fd = fd
        self._handler = handler
        self._notifiers = {}
        self._loop_events = [
            PikaQtLoop.READ,
            PikaQtLoop.WRITE,
            PikaQtLoop.ERROR
        ]

        slots = [self.read_ready, self.write_ready, self.error_ready]

        for i in range(3):
            self._notifiers[i] = QSocketNotifier(fd, i, parent)
            self._notifiers[i].setEnabled(False)
            self._notifiers[i].activated.connect(slots[i], Qt.QueuedConnection)

        self.update_notifier(fd, events)

    def update_notifier(self, _, events):
        for i, event in enumerate(self._loop_events):
            if events & event:
                self._notifiers[i].setEnabled(True)
            else:
                self._notifiers[i].setEnabled(False)

    def remove_notifier(self):
        for i in range(3):
            del self._notifiers[i]

    def read_ready(self, fd):
        if self._notifiers[0].isEnabled():
            self._handler(fd, PikaQtLoop.READ)

    def write_ready(self, fd):
        if self._notifiers[1].isEnabled():
            self._handler(fd, PikaQtLoop.WRITE)

    def error_ready(self, fd):
        if self._notifiers[2].isEnabled():
            self._handler(fd, PikaQtLoop.ERROR)


class PikaQtLoop(QObject, AbstractSelectorIOLoop):
    # READ/WRITE/ERROR per `AbstractSelectorIOLoop` requirements
    READ = getattr(select, 'POLLIN', 0x01)  # available for read
    WRITE = getattr(select, 'POLLOUT', 0x04)  # available for write
    ERROR = getattr(select, 'POLLERR', 0x08)  # error on associated fd

    threadSignal = pyqtSignal()

    def __init__(self):
        super().__init__()
        self.threadSignal.connect(self.process_msg, Qt.QueuedConnection)
        self._callbacks = collections.deque()
        self._timers = []
        self.notifiers = {}
        self.mutex = QMutex()

    def process_msg(self):
        LOGGER.debug('> tid %d', threading.get_ident())
        QMutexLocker(self.mutex)
        # careful: when calling callback, it calls threadsafe func
        # and it blocks by deadlock
        if len(self._callbacks):
            for _ in pika.compat.xrange(len(self._callbacks)):
                callback = self._callbacks.popleft()
                callback()
        LOGGER.debug('< tid %d', threading.get_ident())

    def close(self):
        self._timers.clear()
        self.notifiers.clear()

    def call_later(self, delay, callback):
        timer = QTimer(self)
        self._timers.append(timer)

        timer.setSingleShot(True)
        timer.setInterval(int(delay * 1000))
        timer.timeout.connect(callback, Qt.QueuedConnection)
        timer.start()
        return timer

    def remove_timeout(self, timeout_handle):
        for timer in self._timers:
            if timer == timeout_handle:
                timer.timeout.disconnect()
                timer.stop()
                self._timers.remove(timer)
                break

    def add_callback_threadsafe(self, callback):
        if not callable(callback):
            raise TypeError(
                'callback must be a callable, but got %r' % (callback,))

        LOGGER.debug('>: added callback=%r tid %d',
                     callback, threading.get_ident())
        QMutexLocker(self.mutex)
        self._callbacks.append(callback)
        self.threadSignal.emit()
        LOGGER.debug('<: added callback=%r tid %d',
                     callback, threading.get_ident())

    add_callback = add_callback_threadsafe

    def process_timeouts(self):
        self.process_msg()
        # TODO: handle QTimer pending events

    def add_handler(self, fd, handler, events):
        LOGGER.debug('fd=%d, events=%d', fd, events)

        if fd in self.notifiers:
            self.notifiers[fd].remove_notifier()
            del self.notifiers[fd]

        self.notifiers[fd] = SocketNotifier(self, fd, handler, events)

    def update_handler(self, fd, events):
        LOGGER.debug('fd=%d, events=%d', fd, events)

        if fd in self.notifiers:
            self.notifiers[fd].update_notifier(fd, events)

    def remove_handler(self, fd):
        LOGGER.debug('fd=%d', fd)

        if fd in self.notifiers:
            self.notifiers[fd].remove_notifier()
            del self.notifiers[fd]

    def start(self):
        LOGGER.debug('start, but do nothing since using Qt Eventloop')

    def stop(self):
        LOGGER.debug('stop, but do nothing since using Qt Eventloop')

    def activate_poller(self):
        LOGGER.debug('IOLoop activate_poller')

    def deactivate_poller(self):
        LOGGER.debug('IOLoop deactivate_poller')

    def poll(self):
        LOGGER.debug('IOLoop poll, but do nothing since using Qt Eventloop')
