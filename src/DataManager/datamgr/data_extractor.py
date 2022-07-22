import copy
from datetime import datetime
import os
from typing import Dict, Iterable, List, Tuple
from alpaca_trade_api.rest import REST, TimeFrame
import asyncio
import time
import pandas as pd
import pandas_market_calendars as mcal
from DataManager.utils.timehandler import TimeHandler
from DataManager.assetmgr.asset_manager import Assets
from DataManager.datamgr.historic_async import HistoricalAsync
from DataManager import core
import configparser


class DataExtractor:
    """
    Extracts data given a specific set of stock symbols.

    Example:
    extractor = DataExtractor()
    complete_data, partial_data = extractor.getMultipleListHistoricalAlpaca(required_symbols_data,
        required_dates, TimeFrame.Day, exchangeName)

    Inputs:
        - `None`
    """

    def __init__(self) -> None:
        self.configParse = configparser.ConfigParser()
        self.configParse.read(
            os.path.join(
                core.DATAMGR_ABS_PATH, os.path.join("config_files", "assetConfig.cfg")
            )
        )
        core.setEnv()
        self.AlpacaAPI = REST(raw_data=True)
        self.AsyncObj = HistoricalAsync()

    def getOneHistoricalAlpaca(
        self, symbolName, dateFrom, dateTo, timeframe: TimeFrame, adjustment="all"
    ):
        return self.AlpacaAPI.get_bars(
            symbolName, timeframe, dateFrom, dateTo, adjustment=adjustment
        ).df

    def callHistoricalAlpaca(
        self, listSymbols, dateFrom, dateTo, timeframe: TimeFrame, adjustment="all"
    ):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(
            self.AsyncObj.get_historic_bars(
                listSymbols, dateFrom, dateTo, timeframe, adjustment
            )
        )
        to_return = copy.deepcopy(self.AsyncObj.resultAsync)
        self.AsyncObj.reset_async_list()
        return to_return

    def callHistoricalMultipleAlpaca(
        self, listSymbols, list_dates, timeframe: TimeFrame, adjustment="all"
    ):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(
            self.AsyncObj.get_multiple_dates_historic_bars(
                listSymbols, list_dates, timeframe, adjustment
            )
        )
        to_return = copy.deepcopy(self.AsyncObj.resultAsync)
        self.AsyncObj.reset_async_list()
        return to_return

    def callCalendarAlpaca(self, dateFrom, dateTo):
        return self.AlpacaAPI.get_calendar(dateFrom, dateTo)

    """
        Extracts data from Alpaca asynchronously. Retries if some calls to Alpaca fail.

        Example:
        complete_data, partial_data = getMultipleListHistoricalAlpaca(list_symbols, list_dates, TimeFrame.day,
                                        exchange_name, adjustment='all', maxRetries=3)

        Inputs:
            - `list_symbols`: list of symbols to get data from (can be duplicated)
            - `list_dates`: appropriate dates for the symbols to get data from (indices match with list_symbols list)
            - `timeframe`: timeframe to get data in (days, hours, months)
            - `adjustment`: adjustment for stock data
            - `exchange_name`: exchange to check against for output validation
            - `maxRetries`: number of times to retry Alpaca calls when encountering exceptions
    """

    def getMultipleListHistoricalAlpaca(
        self,
        list_symbols,
        list_dates,
        timeframe: TimeFrame,
        exchange_name,
        adjustment="all",
        maxRetries=3,
    ):
        this_exchange = mcal.get_calendar(exchange_name)
        min_date_timeframe = False
        max_date_timeframe = False
        for i, datePair in enumerate(list_dates):
            if not max_date_timeframe:
                max_date_timeframe = datePair[1]
            if not min_date_timeframe:
                min_date_timeframe = datePair[0]
            max_date_timeframe = max(max_date_timeframe, datePair[1])
            min_date_timeframe = min(min_date_timeframe, datePair[0])
            valid_days = this_exchange.valid_days(datePair[0], datePair[1])
            list_dates[i] = (
                TimeHandler.get_alpaca_string_from_timestamp(valid_days[0]),
                TimeHandler.get_alpaca_string_from_timestamp(valid_days[-1]),
            )

        totalLength = len(
            self.callCalendarAlpaca(min_date_timeframe, max_date_timeframe)
        )
        if totalLength > 1000:
            raise Exception("Alpaca only has data on past 1000 trading days")

        def fix_output(
            list_tuples: Iterable[Tuple[str, pd.DataFrame]]
        ) -> Dict[str, pd.DataFrame]:
            dict_stocks_df = {}
            for individual_tup in list_tuples:
                df_this = individual_tup[1]
                symbol_this = individual_tup[0]
                if symbol_this not in dict_stocks_df:
                    dict_stocks_df[symbol_this] = df_this
                else:
                    dict_stocks_df[symbol_this].update(df_this)
            return dict_stocks_df

        currentRetries = 0
        valid_tuples: List[Tuple[str, pd.DataFrame]] = []
        empty_symbols, partial_symbols = set(), set()
        this_list_symbols = list_symbols
        this_list_dates = list_dates

        while (
            currentRetries <= maxRetries
            and len(valid_tuples) != len(list_symbols)
            and len(this_list_symbols) != 0
        ):
            current_output = self.callHistoricalMultipleAlpaca(
                this_list_symbols, this_list_dates, timeframe, adjustment
            )
            cleaned_output = [
                df_tuple
                for df_tuple in current_output
                if not isinstance(df_tuple, Exception)
            ]
            current_output = fix_output(cleaned_output)
            list_failed_symbols = []
            list_failed_dates = []

            for stock_symbol, date_pair in zip(this_list_symbols, this_list_dates):
                if stock_symbol not in current_output:
                    list_failed_symbols.append(stock_symbol)
                    list_failed_dates.append(date_pair)
                    continue

                fetched_df = current_output[stock_symbol]

                if fetched_df.empty:
                    list_failed_symbols.append(stock_symbol)
                    list_failed_dates.append(date_pair)
                elif (
                    TimeHandler.get_alpaca_string_from_timestamp(fetched_df.index[0]),
                    TimeHandler.get_alpaca_string_from_timestamp(fetched_df.index[-1]),
                ) == date_pair:
                    valid_tuples.append((stock_symbol, fetched_df))
                else:
                    partial_symbols.add(stock_symbol)

            this_list_symbols = list_failed_symbols
            this_list_dates = list_failed_dates
            empty_symbols = set(list_failed_symbols)
            currentRetries += 1
            time.sleep(60)  # 1 min between consecutive requests

        print(f"{len(valid_tuples)=} ,{len(partial_symbols)=}, {len(empty_symbols)=}")
        partial_symbols.update(empty_symbols)
        return valid_tuples, list(partial_symbols)

    def getListHistoricalAlpaca(
        self,
        listSymbols,
        dateFrom,
        dateTo,
        timeframe: TimeFrame,
        adjustment="all",
        maxRetries=3,
    ):
        totalLength = len(self.AlpacaAPI.get_calendar(dateFrom, dateTo))

        if totalLength > 1000:
            raise Exception("Alpaca only has data on past 5 years")

        currentRetries = 0
        thisListSymbols = set(listSymbols)
        initialDfs = []
        while currentRetries <= maxRetries and len(thisListSymbols) != 0:
            currentOutput = self.callHistoricalAlpaca(
                list(thisListSymbols), dateFrom, dateTo, timeframe, adjustment
            )
            initialDfs.extend(
                [e for e in currentOutput if not isinstance(e, Exception)]
            )
            thisSucceededStocks = set(
                [
                    f.__getitem__(0)
                    for f in currentOutput
                    if not isinstance(f, Exception)
                ]
            )
            thisListSymbols = thisListSymbols.difference(thisSucceededStocks)
            currentRetries += 1
        validDfs, partialDfs = [], []

        for df in initialDfs:
            if len(df.__getitem__(1).index) != totalLength:
                partialDfs.append(df)
            else:
                validDfs.append(df)

        return validDfs, partialDfs


if "__main__" == __name__:
    extractor = DataExtractor()
    manager = Assets()
    start = time.time()
    sol, partial = extractor.getListHistoricalAlpaca(
        manager.asset_table_manager.get_all_tradable_symbols()[:10],
        datetime(2017, 6, 1).strftime("%Y-%m-%d"),
        datetime(2021, 2, 1).strftime("%Y-%m-%d"),
        TimeFrame.Day,
    )
    end = time.time()
    print(end - start)
