import json
import logging
from typing import Any, Tuple
from akross.common.enums import Command

from akross.common.exception import MessageParseError


LOGGER = logging.getLogger(__name__)


def _parseMsg(
    body: bytes
) -> Tuple[str, dict]:
    if body is not None and len(body):
        try:
            body = json.loads(body)
            if ('type' in body and
                    'payload' in body and
                    isinstance(body['payload'], dict)):
                return body['type'], body['payload']
        except json.JSONDecodeError as e:
            LOGGER.error('json parse error %s', str(e))

    raise MessageParseError(str(body))


def parseRequestMsg(
    body: bytes
) -> Tuple[str, dict]:
    command, payload = _parseMsg(body)

    if 'args' in payload and isinstance(payload['args'], dict):
        return command, payload['args']
    raise MessageParseError(command, payload)


def parseResponseMsg(
    body: bytes
) -> Tuple[str, Any]:
    command, payload = _parseMsg(body)

    if command == Command.Error:
        msg = payload['msg'] if 'msg' in payload else ''
        LOGGER.warning('get response error \"%s\"', msg)
        return command, None
    else:
        if 'data' in payload:
            return command, payload['data']
    return command, payload
