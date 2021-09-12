from datetime import datetime
import sys
import os
from alpaca_trade_api.rest import REST, TimeFrame
import asyncio

sys.path.insert(0, os.getcwd())  # Resolve Importing errors
from assetmgr.assetmgr_base import AssetManager
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

    def getListHistoricalAlpaca(self, listSymbols, dateFrom, dateTo, timeframe: TimeFrame, adjustment='all'):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.AsyncObj.get_historic_bars(listSymbols, dateFrom, dateTo, timeframe, adjustment))
        return self.AsyncObj.resultAsync


if '__main__' == __name__:
    extractor = DataExtractor()
    manager = AssetManager()
    sol = extractor.getListHistoricalAlpaca(manager.asset_DB.returnAllTradableSymbols(),
                                            datetime(2021, 1, 1).strftime('%Y-%m-%d'),
                                            datetime(2021, 2, 1).strftime('%Y-%m-%d'),
                                            TimeFrame.Day)
    a = [e.__getitem__(0) for e in sol if not isinstance(e, Exception)]
    a = set(a)
    b = set(manager.asset_DB.returnAllTradableSymbols())
    c = list(b.difference(a))
    print()
