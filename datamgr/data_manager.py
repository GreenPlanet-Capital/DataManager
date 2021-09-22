from datetime import datetime, timezone
import sys
import os
import warnings
import numpy as np
import pandas as pd
from alpaca_trade_api.rest import TimeFrame

sys.path.insert(0, os.getcwd())  # Resolve Importing errors
from database_layer.database import DatabaseManager
from assetmgr.asset_manager import Assets
from database_layer.tables import DailyDataTableManager, MainTableManager
from utils.conversions import _Conversions
from utils.timehandler import TimeHandler
from assetmgr.asset_manager import Assets
from datamgr.data_extractor import DataExtractor


class DataManager:
    def __init__(self, exchange_name, asset_db_name='AssetDB.db', stock_db_name='Stock_DataDB.db', update_before=True):
        self.assets = Assets(asset_db_name)
        self.main_stocks = MainStocks(stock_db_name, self.assets)
        self.extractor = DataExtractor()
        if update_before:
            self.assets.update_all_dbs()

        self.exchange_basket = [row_dict['stockSymbol'] for row_dict in
                                self.assets.asset_table_manager.get_exchange_basket(exchange_name, isDelisted=False,
                                                                                    isSuspended=False)]
        self.required_symbols_data, self.required_dates = [], []
        print()

    def reset_required_vars(self):
        self.required_symbols_data, self.required_dates = [], []

    def get_stock_data(self, start_timestamp, end_timestamp, api='Alpaca'):

        for stock in self.exchange_basket:
            self.get_one_stock_data(stock, start_timestamp, end_timestamp)

        list_dicts = getattr(self.extractor, f'getMultipleListHistorical{api}')(self.required_symbols_data,
                                                                                self.required_dates, TimeFrame.Day)
        print()
        self.reset_required_vars()
        self.extractor.AsyncObj.reset_async_list()

    def get_one_stock_data(self, stock_symbol, start_timestamp, end_timestamp):
        statusTimestamp, req_start, req_end = self.main_stocks.table_manager.check_data_availability(stock_symbol,
                                                                                                     start_timestamp,
                                                                                                    end_timestamp)

        if statusTimestamp:
            if req_start:
                self.required_symbols_data.append(stock_symbol)
                self.required_dates.append((TimeHandler.get_alpaca_string_from_string(start_timestamp),
                                            TimeHandler.get_alpaca_string_from_string(
                                                TimeHandler.get_string_from_datetime(req_start))))

            if req_end:
                self.required_symbols_data.append(stock_symbol)
                self.required_dates.append((TimeHandler.get_alpaca_string_from_string(
                                                                            TimeHandler.get_string_from_datetime(req_end)),
                                            TimeHandler.get_alpaca_string_from_string(end_timestamp)))
        else:
            self.required_symbols_data.append(stock_symbol)
            self.required_dates.append((TimeHandler.get_alpaca_string_from_string(start_timestamp),
                                        TimeHandler.get_alpaca_string_from_string(end_timestamp)))


class MainStocks:
    def __init__(self, db_name='Stock_DataDB.db', assets: Assets = None):
        self.table_manager = MainTableManager(f'{os.path.join("tempDir", db_name)}')
        self.assets = assets

    def repopulate_all_assets(self):
        symbols_list = self.assets.asset_table_manager.get_all_tradable_symbols()

        for symbol in symbols_list:
            self.update_stock_symbol_main_table(symbol)

    def update_stock_symbol_main_table(self, stockSymbol, dataAvailableFrom=None, dataAvailableTo=None):
        asset_data = {'stockSymbol': stockSymbol,
                      'dataAvailableFrom': dataAvailableFrom,
                      'dataAvailableTo': dataAvailableTo,
                      'dateLastUpdated': TimeHandler.get_string_from_datetime(datetime.now(timezone.utc))
                      }
        main_asset_data = self.table_manager.get_one_asset(stockSymbol)
        if main_asset_data:
            asset_data['dataAvailableFrom'] = dataAvailableFrom if dataAvailableFrom is not None else main_asset_data[
                'dataAvailableFrom']
            asset_data['dataAvailableTo'] = dataAvailableTo if dataAvailableTo is not None else main_asset_data[
                'dataAvailableTo']
            self.table_manager.update_asset(asset_data)
        else:
            self.table_manager.insert_asset(asset_data)


class DailyStockTables:
    def __init__(self, db_path, main_stocks: MainStocks):
        self.db = DatabaseManager(database_path=db_path)
        self.main_stocks = main_stocks

    def update_daily_stock_data(self, list_of_tuples: list):
        """
        Input: list_of_tuples
        Format: [('SYMBOL1', pandas.Dataframe), ('SYMBOL2', pandas.Dataframe)...]
        """
        # Cannot be threaded
        for stock_symbol, df in list_of_tuples:
            daily_stock_table = DailyStockDataTable(stock_symbol, self.db)
            records = _Conversions().tuples_to_dict(
                list(df.to_records()),
                daily_stock_table.table_manager.columns
            )
            dataAvailableFrom, dataAvailableTo = daily_stock_table.update_daily_stock_data(records)
            self.main_stocks.update_stock_symbol_main_table(stockSymbol=stock_symbol,
                                                            dataAvailableFrom=dataAvailableFrom,
                                                            dataAvailableTo=dataAvailableTo)

    def get_daily_stock_data(self, list_of_symbols, start_timestamp, end_timestamp):
        dictStockData = {}

        for individualSymbol in list_of_symbols:
            thisStockTable = DailyStockDataTable(individualSymbol, self.db).table_manager
            listData = thisStockTable.get_data(start_timestamp, end_timestamp)
            thisDf = pd.DataFrame(listData, columns=list(thisStockTable.columns.keys()))
            dictStockData[individualSymbol] = thisDf

        return dictStockData


class DailyStockDataTable:
    def __init__(self, table_name, db: DatabaseManager):
        self.table_manager = DailyDataTableManager(table_name=table_name, db=db)

    def update_daily_stock_data(self, list_of_timestamped_data: tuple):
        """
        Accepts a tuple of dictionaries of timestamped OHLCVTV data
        Returns: tuple with the new date available from and date available to
        """
        for timestamp_ohlc_dict in list_of_timestamped_data:
            if isinstance(timestamp_ohlc_dict['timestamp'], np.datetime64):
                timestamp_ohlc_dict['timestamp'] = \
                    TimeHandler.get_string_from_datetime64(timestamp_ohlc_dict['timestamp'])
            if not self.table_manager.get_one_day_data(timestamp_ohlc_dict['timestamp']):
                self.table_manager.insert_data(timestamp_ohlc_dict)
            else:
                warnings.warn(
                    f'Trying to insert OHLCVTC data that already exists for timestamp: {timestamp_ohlc_dict["timestamp"]}')

        return self.table_manager.get_dates_for_available_data()


if __name__ == '__main__':
    assets = Assets('AssetDB.db')
    # assets.update_all_dbs()
    main_stocks = MainStocks('Stock_DataDB.db', assets)
    # main_stocks.repopulate_all_assets()

    data = DataManager('NYSE', update_before=False)
    data.get_stock_data(TimeHandler.get_string_from_datetime(datetime(2017, 6, 1)),
                        TimeHandler.get_string_from_datetime(datetime(2017, 7, 1)))
    print()

    # dbAddr = f'{os.path.join("tempDir", "Stock_DataDB_Test.db")}'
    # db_manager = DatabaseManager(dbAddr)
    # test_symbol_stock_table = DailyStockDataTable('TEST_SYMBOL_TWO', db_manager)
    # main_stocks = MainStocks('Stock_DataDB_Test.db')

    # listTimestampedData = ({'timestamp': '2021-01-01',
    #                         'open': 150.5655,
    #                         'high': 155.5655,
    #                         'low': 145.5909,
    #                         'close': 148.5390,
    #                         'volume': 2003940,
    #                         'trade_count': 45000,
    #                         'vwap': None,
    #                         },
    #                        {'timestamp': '2021-01-02',
    #                         'open': 152.5655,
    #                         'high': 157.5655,
    #                         'low': 147.5909,
    #                         'close': 150.5390,
    #                         'volume': 345_000,
    #                         'trade_count': 50_000,
    #                         'vwap': None, },
    #                        {'timestamp': '2021-01-03',
    #                         'open': 154.5655,
    #                         'high': 159.5655,
    #                         'low': 149.5909,
    #                         'close': 151.5390,
    #                         'volume': 100_000,
    #                         'trade_count': 35_000,
    #                         'vwap': None, },
    #                        )
    # dFrom, dTo = test_symbol_stock_table.update_daily_stock_data(
    #     list_of_timestamped_data=listTimestampedData)
    # main_stocks.update_stock_symbol_main_table(test_symbol_stock_table.table_manager.table_name,
    #                                            dataAvailableFrom=dFrom, dataAvailableTo=dTo)

    # allStockTables = DailyStockTables(dbAddr, main_stocks)
    # allStockTables.get_daily_stock_data(['TEST_SYMBOL_TWO'], '2021-01-01', '2021-01-03')
