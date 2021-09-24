import copy
from datetime import datetime
import sys
import os
from alpaca_trade_api.rest import REST, TimeFrame
import asyncio
import time
import pandas_market_calendars as mcal
from utils.timehandler import TimeHandler
import pandas as pd
from database_layer.tables import DailyDataTableManager

sys.path.insert(0, os.getcwd())  # Resolve Importing errors
from assetmgr.asset_manager import Assets
from datamgr.historic_async import HistoricalAsync, DataType
from core import *


class DataExtractor:
    def __init__(self) -> None:
        self.configParse = configparser.ConfigParser()
        self.configParse.read(os.path.join('config_files', 'assetConfig.cfg'))
        setEnv()
        self.AlpacaAPI = REST(raw_data=True)
        self.AsyncObj = HistoricalAsync()

    def getOneHistoricalAlpaca(self, symbolName, dateFrom, dateTo, timeframe: TimeFrame, adjustment='all'):
        return self.AlpacaAPI.get_bars(symbolName, timeframe, dateFrom, dateTo, adjustment=adjustment).df

    def callHistoricalAlpaca(self, listSymbols, dateFrom, dateTo, timeframe: TimeFrame, adjustment='all'):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.AsyncObj.get_historic_bars(listSymbols, dateFrom, dateTo, timeframe, adjustment))
        to_return = copy.deepcopy(self.AsyncObj.resultAsync)
        self.AsyncObj.reset_async_list()
        return to_return

    def callHistoricalMultipleAlpaca(self, listSymbols, list_dates, timeframe: TimeFrame, adjustment='all'):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.AsyncObj.get_multiple_dates_historic_bars(listSymbols, list_dates, timeframe,
                                                                               adjustment))
        to_return = copy.deepcopy(self.AsyncObj.resultAsync)
        self.AsyncObj.reset_async_list()
        return to_return

    def getMultipleListHistoricalAlpaca(self, list_symbols, list_dates, timeframe: TimeFrame, adjustment='all',
                                        exchange_name='NYSE', maxRetries=3):
        date_ranges = {}

        this_exchange = mcal.get_calendar(exchange_name)
        for i, datePair in enumerate(list_dates):
            if list_symbols[i] in date_ranges:
                date_ranges[list_symbols[i]].append(this_exchange.valid_days(datePair[0], datePair[1]))
            else:
                date_ranges[list_symbols[i]] = [this_exchange.valid_days(datePair[0], datePair[1])]
        len_list_dates = ([max(len(dates[0]), len(dates[-1])) for dates in list(date_ranges.values())])
        if max(len_list_dates) > 1000:
            raise Exception('Alpaca only has data on past 5 years')

        def fix_output(list_tuples):
            dict_stocks_df = {}
            for individual_tup in list_tuples:
                df_this = individual_tup[1]
                symbol_this = individual_tup[0]
                if symbol_this not in dict_stocks_df:
                    dict_stocks_df[symbol_this] = [df_this]
                else:
                    dict_stocks_df[symbol_this].append(df_this)
            return dict_stocks_df

        currentRetries = 0
        valid_tuples, empty_symbols, partial_symbols = [], set(), set()
        this_list_symbols = list_symbols
        this_list_dates = list_dates

        while currentRetries <= maxRetries and len(valid_tuples) != len(list_symbols):
            current_output = self.callHistoricalMultipleAlpaca(this_list_symbols,
                                                               this_list_dates, timeframe,
                                                               adjustment)
            current_output = fix_output(current_output)
            list_failed_symbols = []
            list_failed_dates = []

            for stock_symbol, date_pair in zip(this_list_symbols, this_list_dates):
                if stock_symbol not in current_output:
                    list_failed_symbols.append(stock_symbol)
                    list_failed_dates.append(date_pair)
                    continue
                fetched_dfs = current_output[stock_symbol]

                for one_df in fetched_dfs:
                    if one_df.empty:
                        empty_symbols.add(stock_symbol)
                    elif (TimeHandler.get_alpaca_string_from_timestamp(one_df.index[0]),
                          TimeHandler.get_alpaca_string_from_timestamp(one_df.index[-1])) == date_pair:
                        valid_tuples.append((stock_symbol, one_df))
                    else:
                        partial_symbols.add(stock_symbol)

                currentRetries += 1

            this_list_symbols = list_failed_symbols
            this_list_dates = list_failed_dates

        print(len(valid_tuples), len(partial_symbols), len(empty_symbols))
        partial_symbols.update(empty_symbols)
        return valid_tuples, list(partial_symbols)

    def getListHistoricalAlpaca(self, listSymbols, dateFrom, dateTo, timeframe: TimeFrame, adjustment='all',
                                maxRetries=3):
        totalLength = len(self.AlpacaAPI.get_calendar(dateFrom, dateTo))

        if totalLength > 1000:
            raise Exception('Alpaca only has data on past 5 years')

        currentRetries = 0
        thisListSymbols = set(listSymbols)
        initialDfs = []
        while currentRetries <= maxRetries and len(thisListSymbols) != 0:
            currentOutput = self.callHistoricalAlpaca(list(thisListSymbols), dateFrom, dateTo, timeframe, adjustment)
            initialDfs.extend([e for e in currentOutput if not isinstance(e, Exception)])
            thisSucceededStocks = set([f.__getitem__(0) for f in currentOutput if not isinstance(f, Exception)])
            thisListSymbols = thisListSymbols.difference(thisSucceededStocks)
            currentRetries += 1
        validDfs, partialDfs = [], []

        for df in initialDfs:
            if len(df.__getitem__(1).index) != totalLength:
                partialDfs.append(df)
            else:
                validDfs.append(df)

        return validDfs, partialDfs


if '__main__' == __name__:
    extractor = DataExtractor()
    manager = Assets()
    start = time.time()
    sol, partial = extractor.getListHistoricalAlpaca(manager.asset_table_manager.get_all_tradable_symbols()[:10],
                                                     datetime(2017, 6, 1).strftime('%Y-%m-%d'),
                                                     datetime(2021, 2, 1).strftime('%Y-%m-%d'),
                                                     TimeFrame.Day)
    end = time.time()
    print(end - start)
