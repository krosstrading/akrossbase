import asyncio

from akross.connection.client import ClientConnection


async def main():
    await conn.connect()
    await conn.market_discovery()
    await conn.wait_for_market('hello.spot')
    hellos = conn.get_markets('hello.spot')
    resp = await conn.api_call(hellos[0], 'candle', interval='3m')
    print('candle', resp)
    resp = await conn.api_call(hellos[0], 'priceStream', target='BTCUSDT')
    print('price_stream', resp)
    
    await asyncio.get_running_loop().create_future()


if __name__ == '__main__':
    import logging
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    conn = ClientConnection('hello.spot', '127.0.0.1')
    asyncio.run(main())