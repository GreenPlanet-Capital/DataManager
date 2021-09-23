import copy
from datetime import datetime
import sys
import os
from alpaca_trade_api.rest import REST, TimeFrame
import asyncio
import time
import pandas_market_calendars as mcal
from utils.timehandler import TimeHandler

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

    # TODO - Clean up getList methods
    def getMultipleListHistoricalAlpaca(self, listSymbols, listDates, timeframe: TimeFrame, adjustment='all',
                                        exchange_name='NYSE', maxRetries=3):
        # TODO: Put sorting in helper function
        listSymbols, listDates = (list(t) for t in zip(*sorted(zip(listSymbols, listDates),
                                                               key=lambda x: (x[0], x[1][0]))))
        date_ranges = {}
        thisExchange = mcal.get_calendar(exchange_name)
        for i, datePair in enumerate(listDates):
            if listSymbols[i] in date_ranges:
                date_ranges[listSymbols[i]].append(thisExchange.valid_days(datePair[0], datePair[1]))
            else:
                date_ranges[listSymbols[i]] = [thisExchange.valid_days(datePair[0], datePair[1])]
        len_list_dates = ([len(dates[0]) for dates in list(date_ranges.values())])
        if max(len_list_dates) > 1000:
            raise Exception('Alpaca only has data on past 5 years')

        currentRetries = 0
        this_list_symbols = listSymbols
        this_list_dates = listDates
        initial_list = []
        partial_df = []

        while currentRetries <= maxRetries and len(this_list_symbols) != 0:
            current_output = self.callHistoricalMultipleAlpaca(list(this_list_symbols), this_list_dates, timeframe,
                                                               adjustment)
            cleaned_output = []
            for elem in current_output:
                if not isinstance(elem, Exception):
                    if not elem[1].empty:
                        cleaned_output.append(elem)
                    else:
                        # Appending empty dfs only
                        partial_df.append(elem[0])

            cleaned_output.sort(key=lambda x: (x[0], TimeHandler.get_alpaca_string_from_timestamp(x[1].index[0])))
            initial_list.extend(cleaned_output)

            iterator_cleaned = 0
            symbols_failed, dates_failed = [], []
            for current_symbol in this_list_symbols:
                if iterator_cleaned>=len(cleaned_output):
                    break
                list_dates = date_ranges[current_symbol]

                list_dates1 = (TimeHandler.get_alpaca_string_from_timestamp(list_dates[0][0]),
                               TimeHandler.get_alpaca_string_from_timestamp(list_dates[0][-1]))
                list_dates2 = (TimeHandler.get_alpaca_string_from_timestamp(list_dates[-1][0]),
                               TimeHandler.get_alpaca_string_from_timestamp(list_dates[-1][-1]))
                this_cleaned_date_tuple = (
                    TimeHandler.get_alpaca_string_from_datetime(cleaned_output[iterator_cleaned][1].index[0].date()),
                    TimeHandler.get_alpaca_string_from_datetime(cleaned_output[iterator_cleaned][1].index[-1].date()))
                if current_symbol == cleaned_output[iterator_cleaned][0] and this_cleaned_date_tuple in \
                        [list_dates1, list_dates2]:
                    iterator_cleaned += 1
                    continue
                else:
                    if not current_symbol in partial_df:
                        symbols_failed.append(current_symbol)
                        dates_failed.append(this_cleaned_date_tuple)

            currentRetries += 1
            this_list_symbols = symbols_failed
            this_list_dates = dates_failed

        validDfs = []
        partial_df.extend(this_list_symbols)

        for this_symbol, df in initial_list:
            date_range_item = date_ranges[this_symbol]
            parsed_first = TimeHandler.get_alpaca_string_from_datetime(df.index[0].date())
            parsed_last = TimeHandler.get_alpaca_string_from_datetime(df.index[-1].date())

            if parsed_first in [TimeHandler.get_alpaca_string_from_timestamp(date_range_item[0][0]), 
                                TimeHandler.get_alpaca_string_from_timestamp(date_range_item[0][-1])] or parsed_last in \
                                [TimeHandler.get_alpaca_string_from_timestamp(date_range_item[-1][0]), 
                                TimeHandler.get_alpaca_string_from_timestamp(date_range_item[-1][-1])]:
                validDfs.append((this_symbol, df))
            else:
                partial_df.append(this_symbol)
        print(len(validDfs), len(partial_df))
        return validDfs, partial_df

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
    print()
