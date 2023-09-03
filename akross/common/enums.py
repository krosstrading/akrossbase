from enum import Enum


class Command(str, Enum):
    Error = 'error'
    OK = 'OK'
    Pong = 'pong'
    Connect = 'connect'
    Discovery = 'discovery'
    CreateBacktest = 'createBacktest'
    FinishBacktest = 'finishBacktest'
    Search = 'search'


class PredefinedExchange:
    Akrossquote = 'akrossquote'
    Akrossaccount = 'akrossaccount'
    Akrossplanner = 'akrossplanner'


class CommandType(str, Enum):
    ReqRes = 'reqres'
    Subscribe = 'subscribe'


class WorkerType(str, Enum):
    Online = 'online'
    Cache = 'cache'
    Backtest = 'backtest'


class BacktestType(str, Enum):
    Interval = 'interval'
    Stream = 'stream'


class WorkerSuffix:
    Broker = '.broker'
    Cache = '.cache'
