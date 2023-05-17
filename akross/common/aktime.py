from datetime import datetime, timezone, timedelta
import pytz

try:
    from time import time_ns
except ImportError:
    def time_ns():
        now = datetime.now()
        return int(now.timestamp() * 1e9)


DEFAULT_MIN_INTERVAL = '1m'
DEFAULT_DAY_INTERVAL = '1d'
MARKET_TIMEZONE = {
    'KRX': pytz.timezone("Asia/Seoul"),
    'ASIA/SEOUL': pytz.timezone("Asia/Seoul")
}


DEFAULT_INTERVAL_COUNT = 1000

_interval_ms = {
    's': 1000,
    'm': 1000 * 60,
    'h': 1000 * 60 * 60,
    'd': 1000 * 60 * 60 * 24,
    'w': 1000 * 60 * 60 * 24 * 7,
    'M': 1000 * 60 * 60 * 24 * 30,  # BUG, 30일 고정 아님
    'y': 1000 * 60 * 60 * 24 * 365
}

_default_interval_count = {
    's': 1000,
    'm': 5000,  # for KRX market, approximately 3 days
    'h': 1000,
    'd': 1000,
    'w': 200,
    'M': 200,
    'y': 20
}


def interval_to_msec(interval: str) -> int:
    if len(interval) > 1:
        try:
            unit = interval[-1]
            value = int(interval[:-1])

            if unit in _interval_ms:
                return _interval_ms[unit] * value
        except Exception:
            pass

    return 0


def interval_type_to_msec(interval_type: str) -> int:
    if interval_type in _interval_ms:
        return _interval_ms[interval_type]
    return 0


def get_msec() -> int:
    return int(time_ns() / 1000000)


def get_from_ms(interval, until_ms) -> int:
    unit_ms = interval_to_msec(interval) * \
              _default_interval_count[interval[-1]]
    return until_ms - unit_ms


def msec_diff(msec):
    return datetime.now() - datetime.fromtimestamp(int(msec / 1000))


def msec_to_string(msec):
    return datetime.fromtimestamp(int(msec / 1000), timezone.utc)


def interval_dissect(interval):
    return int(interval[:-1]), interval[-1]


def msec_to_yyyymmdd(msec, market=''):
    dt = datetime.fromtimestamp(int(msec / 1000))

    if len(market) > 0 and market.upper() in MARKET_TIMEZONE:
        dt = dt.astimezone(MARKET_TIMEZONE[market.upper()])
    else:
        dt = dt.astimezone(timezone.utc)

    return int(f'{dt.year}{str(dt.month).zfill(2)}{str(dt.day).zfill(2)}')


def msec_to_datetime(msec, market=''):
    dt = datetime.fromtimestamp(int(msec / 1000))

    if len(market) > 0 and market.upper() in MARKET_TIMEZONE:
        dt = dt.astimezone(MARKET_TIMEZONE[market.upper()])
    else:
        dt = dt.astimezone(timezone.utc)

    return dt


def datetime_to_msec(dt):
    # below conditional has no meaning and return same msec
    # if len(market) and market.upper() in MARKET_TIMEZONE:
    #     dt = dt.astimezone(MARKET_TIMEZONE[market.upper()])
    # else:
    #     dt = dt.astimezone(timezone.utc)
    return int(dt.timestamp() * 1000)


def intdate_to_tuple(intdate: int):
    year = int(intdate / 10000)
    month = int((intdate % 10000) / 100)
    day = intdate % 100
    return year, month, day


def intdatetime_to_tuple(intdatetime: int):
    year, month, day = intdate_to_tuple(int(intdatetime / 1000000))
    time = intdatetime % 1000000
    hour = int(time / 10000)
    min = int((time % 10000) / 100)
    sec = time % 100
    return year, month, day, hour, min, sec


def get_datetime_now(market=''):
    if len(market) > 0 and market.upper() in MARKET_TIMEZONE:
        return datetime.now(MARKET_TIMEZONE[market.upper()])
    return datetime.now(pytz.utc)


def inttime_to_datetime(inttime: int, market=''):
    # inttime is 900, 1600
    hour = int(inttime / 100)
    minute = int(inttime % 100)
    if len(market) > 0 and market.upper() in MARKET_TIMEZONE:
        dt = datetime.now(MARKET_TIMEZONE[market.upper()])
        return MARKET_TIMEZONE[market.upper()].localize(
            datetime(dt.year, dt.month, dt.day, hour, minute)
        )

    dt = datetime.now(pytz.utc)
    return pytz.timezone('UTC').localize(
        datetime(dt.year, dt.month, dt.day, hour, minute)
    )


def intdate_to_datetime(intdate: int, market=''):
    year, month, day = intdate_to_tuple(intdate)

    if year < 1970:
        return datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    if len(market) and market.upper() in MARKET_TIMEZONE:
        dt = MARKET_TIMEZONE[market.upper()].localize(
            datetime(year, month, day)
        )
    else:
        dt = pytz.timezone('UTC').localize(datetime(year, month, day))
    return dt


# https://stackoverflow.com/questions/18541051/datetime-and-timezone-conversion-with-pytz-mind-blowing-behaviour
# datetime(xxxx, tzinfo=xxx) has bugs
def intdate_to_msec(intdate: int, market=''):
    dt = intdate_to_datetime(intdate, market)
    return int(dt.timestamp() * 1000)


# https://stackoverflow.com/questions/18541051/datetime-and-timezone-conversion-with-pytz-mind-blowing-behaviour
# datetime(xxxx, tzinfo=xxx) has bugs
def intdatetime_to_msec(intdatetime: int, market=''):
    year, month, day, hour, min, sec = intdatetime_to_tuple(intdatetime)
    if year == 0:
        return 0
    if len(market) and market.upper() in MARKET_TIMEZONE:
        dt = MARKET_TIMEZONE[market.upper()].localize(
            datetime(year, month, day, hour, min, sec)
        )
    else:
        dt = pytz.timezone('UTC').localize(
            datetime(year, month, day, hour, min, sec)
        )
    return int(dt.timestamp() * 1000)


def eod_of_intdate_to_msec(intdate, market=''):
    return intdate_to_msec(intdate, market) + _interval_ms['d'] - 1


def get_start_time(ms: int, interval_type: str, market=''):
    if interval_type == 'm':
        return ms - ms % interval_type_to_msec(interval_type)
    else:
        dt = msec_to_datetime(ms, market)
        tz = pytz.timezone('UTC')
        if len(market) > 0 and market.upper() in MARKET_TIMEZONE:
            tz = MARKET_TIMEZONE[market.upper()]

        if interval_type == 'h':
            dt = tz.localize(datetime(dt.year, dt.month, dt.day, dt.hour))
        elif interval_type == 'd':
            dt = tz.localize(datetime(dt.year, dt.month, dt.day))
        elif interval_type == 'w':
            dt = tz.localize(
                datetime(dt.year, dt.month, dt.day) - timedelta(days=dt.weekday())
            )
        elif interval_type == 'M':
            dt = tz.localize(datetime(dt.year, dt.month, 1))
        elif interval_type == 'y':
            dt = tz.localize(datetime(dt.year, 1, 1))
    return int(dt.timestamp() * 1000)


def get_end_time(start_msec: int, interval_type: str, market=''):
    if interval_type == 'M':
        # start_msec 이 월의 시작이라고 가정하면, 32일 이후는 반드시 다음 달로 넘어감
        return get_start_time(start_msec + interval_type_to_msec('d') * 32, 'M', market) - 1

    return start_msec + interval_type_to_msec(interval_type) - 1


def get_msec_before_day(days: int, current: int = 0):
    """
    get milliseconds before days from now
    """
    now = get_msec() if current == 0 else current
    return now - interval_type_to_msec('d') * days


def get_yesterday_eod(msec: int, market=''):
    dt = msec_to_datetime(msec, market)
    tz = pytz.timezone('UTC')
    if len(market) > 0 and market.upper() in MARKET_TIMEZONE:
        tz = MARKET_TIMEZONE[market.upper()]

    dt = tz.localize(datetime(dt.year, dt.month, dt.day))
    return int(dt.timestamp() * 1000) - 1


# be careful to use tzinfo in datetime constructor -> timestamp not matched,
# use astimezone
if __name__ == '__main__':
    dt = msec_to_datetime(get_msec(), 'KRX')
    print(dt.hour)
    print(get_datetime_now('KRX'))
    print(get_datetime_now())
    print(inttime_to_datetime(900, 'KRX'))
    print(inttime_to_datetime(900))
    # local_time = datetime(2023, 1, 10, 8).timestamp() * 1000
    # dt = msec_to_datetime(local_time, 'KRX')
    # assert dt.hour == 8
    # dt = msec_to_datetime(local_time)
    # assert dt.hour == 23
    # assert msec_to_yyyymmdd(local_time, 'KRX') == 20230110
    # assert msec_to_yyyymmdd(local_time) == 20230109

    # print(msec_to_datetime(datetime(2023, 1, 10).timestamp() * 1000, 'KRX'))
    # assert start_of_day(local_time) == intdate_to_msec(20230109)
    # msec = intdatetime_to_msec(20230110090000, 'KRX')
    # print('local msec_to_dt', msec_to_datetime(local_time, 'KRX'))
    # print(msec_to_datetime(msec, 'KRX'))
    # msec_to_string(1623715200000)
    # msec_to_string(1623801599999)

    # print(msec_to_yyyymmdd(1623801599999))
    # print(msec_to_yyyymmdd(1623801599999, 'KRX'))

    # print(msec_to_yyyymmdd(get_from_ms('3m', get_msec()), 'KRX'))
    # print(msec_to_yyyymmdd(get_from_ms('1d', get_msec()), 'KRX'))
    # print(msec_to_yyyymmdd(get_from_ms('1w', get_msec()), 'KRX'))
    # print(msec_to_yyyymmdd(get_from_ms('1M', get_msec()), 'KRX'))
