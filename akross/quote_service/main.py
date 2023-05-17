import asyncio
import logging
import sys

from akross.quote_service.msg_broker import MsgBroker


async def main(market_name: str):
    msg_broker = MsgBroker(market_name)
    await msg_broker.run()
    await asyncio.get_running_loop().create_future()
    print('done')


if __name__ == '__main__':
    LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
                  '-35s %(lineno) -5d: %(message)s')
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
    if len(sys.argv) < 2:
        print('market name is missing in arguments')
        sys.exit(1)
    asyncio.run(main(sys.argv[1]))
