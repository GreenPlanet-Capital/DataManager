from enum import Enum
import alpaca_trade_api as tradeapi
import asyncio
import sys
from alpaca_trade_api.rest import TimeFrame, URL
from alpaca_trade_api.rest_async import gather_with_concurrency, AsyncRest
from core import *


class DataType(str, Enum):
    Bars = "Bars"
    Trades = "Trades"
    Quotes = "Quotes"


class HistoricalAsync:
    def __init__(self):
        setEnv()
        api_key_id = os.environ.get('APCA_API_KEY_ID')
        api_secret = os.environ.get('APCA_API_SECRET_KEY')
        base_url = os.environ.get('APCA_API_BASE_URL')

        self.rest = AsyncRest(key_id=api_key_id,
                              secret_key=api_secret)

        self.api = tradeapi.REST(key_id=api_key_id,
                                 secret_key=api_secret,
                                 base_url=URL(base_url))

        self.resultAsync = []

    def reset_async_list(self):
        self.resultAsync = []

    def get_data_method(self, data_type: DataType):
        if data_type == DataType.Bars:
            return self.rest.get_bars_async
        elif data_type == DataType.Trades:
            return self.rest.get_trades_async
        elif data_type == DataType.Quotes:
            return self.rest.get_quotes_async
        else:
            raise Exception(f"Unsupoported data type: {data_type}")

    async def get_historic_data_base(self, symbols, data_type: DataType, start, end,
                                     timeframe: TimeFrame = None, adjustmentInput='raw'):
        """
        base function to use with all
        :param adjustmentInput:
        :param symbols:
        :param start:
        :param end:
        :param timeframe:
        :return:
        """
        major = sys.version_info.major
        minor = sys.version_info.minor
        if major < 3 or minor < 6:
            raise Exception('asyncio is not support in your python version')
        msg = f"Getting {data_type} data for {len(symbols)} symbols"
        msg += f", timeframe: {timeframe}" if timeframe else ""
        msg += f" between dates: start={start}, end={end}"
        print(msg)

        tasks = []

        for symbol in symbols:
            args = [symbol, start, end, timeframe] if timeframe else \
                [symbol, start, end]
            tasks.append(self.get_data_method(data_type)(*args, adjustment=adjustmentInput))

        if minor >= 8:
            results = await asyncio.gather(*tasks, return_exceptions=True)
        else:
            results = await gather_with_concurrency(500, *tasks)

        bad_requests = 0
        for response in results:
            if isinstance(response, Exception):
                print(f"Got an error: {response}")
            elif not len(response[1]):
                bad_requests += 1

        print(f"Total of {len(results)} {data_type}, and {bad_requests} "
              f"empty responses.")

        return results

    async def get_historic_data_multiple_base(self, symbols, data_type: DataType, list_dates,
                                              timeframe: TimeFrame = None, adjustmentInput='raw'):
        """
        base function to use with all
        :param adjustmentInput:
        :param symbols:
        :param start:
        :param end:
        :param timeframe:
        :return:
        """
        major = sys.version_info.major
        minor = sys.version_info.minor
        if major < 3 or minor < 6:
            raise Exception('asyncio is not support in your python version')
        msg = f"Getting {data_type} data for {len(symbols)} symbols"
        msg += f", timeframe: {timeframe}" if timeframe else ""
        msg += f" between dates specified in the list"
        print(msg)

        tasks = []

        for i, symbol in enumerate(symbols):
            args = [symbol, list_dates[i][0], list_dates[i][1], timeframe] if timeframe else \
                [symbol, list_dates[i][0], list_dates[i][1]]
            tasks.append(self.get_data_method(data_type)(*args, adjustment=adjustmentInput))

        if minor >= 8:
            results = await asyncio.gather(*tasks, return_exceptions=True)
        else:
            results = await gather_with_concurrency(500, *tasks)

        bad_requests = 0
        for response in results:
            if isinstance(response, Exception):
                print(f"Got an error: {response}")
            elif not len(response[1]):
                bad_requests += 1

        print(f"Total of {len(results)} {data_type}, and {bad_requests} "
              f"empty responses.")

        return results

    async def get_historic_bars(self, symbols, start, end, timeframe: TimeFrame, adjustmentInput='raw'):
        self.resultAsync = await self.get_historic_data_base(symbols, DataType.Bars, start, end, timeframe,
                                                             adjustmentInput)

    async def get_multiple_dates_historic_bars(self, symbols, list_dates, timeframe: TimeFrame, adjustmentInput='raw'):
        self.resultAsync = await self.get_historic_data_multiple_base(symbols, DataType.Bars, list_dates, timeframe,
                                                                      adjustmentInput)

    async def get_historic_trades(self, symbols, start, end, timeframe: TimeFrame):
        await self.get_historic_data_base(symbols, DataType.Trades, start, end)

    async def get_historic_quotes(self, symbols, start, end, timeframe: TimeFrame):
        await self.get_historic_data_base(symbols, DataType.Quotes, start, end)
