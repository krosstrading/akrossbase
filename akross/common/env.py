import os


def _get_env(key: str, default: str) -> str:
    value = default
    env = os.getenv(key)
    if env:
        value = env
    return value


def get_rmq_url() -> str:
    return _get_env('RMQ_URL', '172.21.112.1')


def get_rmq_user() -> str:
    return _get_env('RMQ_USER', 'akross')


def get_rmq_password() -> str:
    return _get_env('RMQ_PASSWORD', 'Akross@q')
