from datetime import datetime
import sys
import os
from alpaca_trade_api.rest import REST, TimeFrame
import asyncio
import time

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
        return self.AsyncObj.resultAsync[0]

    def callHistoricalMultipleAlpaca(self, listSymbols, list_dates, timeframe: TimeFrame, adjustment='all'):
        loop = asyncio.get_event_loop()
        list_async_objects = []

        for symbol, date_pair in zip(listSymbols, list_dates):
            list_async_objects.append(self.AsyncObj.get_historic_bars([symbol], date_pair[0], date_pair[1],
                                                                      timeframe, adjustment))

        for async_obj in list_dates:
            loop.run_until_complete(async_obj)

        return self.AsyncObj.resultAsync

    # TODO - Clean up getList methods
    def getMultipleListHistoricalAlpaca(self, listSymbols, listDates, timeframe: TimeFrame, adjustment='all',
                                        maxRetries=3):
        totalLength = []
        for datePair in listDates:
            totalLength.append(self.AlpacaAPI.get_calendar(datePair[0], datePair[1]))

        if max(totalLength) > 1000:
            raise Exception('Alpaca only has data on past 5 years')

        currentRetries = 0
        thisListSymbols = set(listSymbols)
        initialDfs = []
        while currentRetries <= maxRetries and len(thisListSymbols) != 0:
            currentOutput = self.callHistoricalMultipleAlpaca(list(thisListSymbols), listDates, timeframe, adjustment)
            self.AsyncObj.reset_async_list()
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
            self.AsyncObj.reset_async_list()
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
    sol, partial = extractor.getListHistoricalAlpaca(manager.asset_table_manager.get_all_tradable_symbols(),
                                                     datetime(2017, 6, 1).strftime('%Y-%m-%d'),
                                                     datetime(2021, 2, 1).strftime('%Y-%m-%d'),
                                                     TimeFrame.Day)
    end = time.time()
    print(end - start)
    print()
