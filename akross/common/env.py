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


def get_mongo_url() -> str:
    mongo_url = _get_env('MONGO_QUOTE_URL', '')
    if len(mongo_url) == 0:
        return get_rmq_url()
    return mongo_url


def get_mongo_user() -> str:
    mongo_user = _get_env('MONGO_QUOTE_USER', '')
    if len(mongo_user) == 0:
        return get_rmq_user()
    return mongo_user


def get_mongo_password() -> str:
    password = _get_env('MONGO_QUOTE_PASSWORD', '')
    if len(password) == 0:
        return get_rmq_password()
    return password


def get_mongo_stream_url() -> str:
    mongo_url = _get_env('MONGO_STREAM_URL', '')
    if len(mongo_url) == 0:
        return get_mongo_url()
    return mongo_url
