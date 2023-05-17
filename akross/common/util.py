import json
import logging
from typing import Any, Tuple, Union
import aio_pika

from .enums import Command
from .exception import MessageParseError, ArgumentError


LOGGER = logging.getLogger(__name__)


def _parseMsg(
    msg: aio_pika.abc.AbstractIncomingMessage
) -> Tuple[str, dict]:
    if msg is not None and len(msg.body):
        try:
            body = json.loads(msg.body)
            if ('type' in body and
                    'payload' in body and
                    isinstance(body['payload'], dict)):
                return body['type'], body['payload']
        except json.JSONDecodeError as e:
            LOGGER.error('json parse error %s', str(e))

    raise MessageParseError(str(msg.body))


def parseRequestMsg(
    msg: aio_pika.abc.AbstractIncomingMessage
) -> Tuple[str, dict]:
    command, payload = _parseMsg(msg)

    if 'args' in payload and isinstance(payload['args'], dict):
        return command, payload['args']
    raise MessageParseError(command, payload)


def parseResponseMsg(
    msg: aio_pika.abc.AbstractIncomingMessage
) -> Tuple[str, Any]:
    command, payload = _parseMsg(msg)

    if command == Command.Error:
        msg = payload['msg'] if 'msg' in payload else ''
        LOGGER.warning('get response error \"%s\"', msg)
        return command, None
    else:
        if 'data' in payload:
            return command, payload['data']
    return command, payload


def check_required_parameters(args: dict, *params):
    for p in params:
        if p not in args:
            raise ArgumentError(f'{p} not in argument')


def encodeRequestMsg(command, **kwargs) -> bytes:
    cmd = command.value if isinstance(command, Command) else command
    return json.dumps({
        'type': cmd, 'payload': {'args': kwargs}
    }).encode()


def encodeResponseError(msg: str) -> bytes:
    ret = {
        'type': Command.Error.value,
        'payload': {'msg': msg}
    }
    return json.dumps(ret).encode()


def encodeResponseMsg(command: Union[str, Command], data: Any) -> bytes:
    cmd = command.value if isinstance(command, Command) else command
    ret = {
        'type': cmd,
        'payload': {'data': data}
    }
    return json.dumps(ret).encode()


def encodeBacktestStream(command: Union[str, Command], target: str, data: Any) -> bytes:
    cmd = command.value if isinstance(command, Command) else command
    ret = {
        'type': cmd,
        'payload': {'data': {'command': cmd, 'target': target, 'stream': data}}
    }
    return json.dumps(ret).encode()