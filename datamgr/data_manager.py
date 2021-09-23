from datetime import datetime, timezone
import sys
import os
import warnings
import numpy as np
import pandas as pd
from alpaca_trade_api.rest import TimeFrame
import pandas_market_calendars as mcal

sys.path.insert(0, os.getcwd())  # Resolve Importing errors
from database_layer.database import DatabaseManager
from assetmgr.asset_manager import Assets
from database_layer.tables import DailyDataTableManager, MainTableManager
from utils.conversions import _Conversions
from utils.timehandler import TimeHandler
from assetmgr.asset_manager import Assets
from datamgr.data_extractor import DataExtractor


class DataManager:
    """
    Provides financial data for a certain set of stock symbols.

    Example:
    data = DataManager(limit=10, update_before=True, exchangeName = 'NYSE', isDelisted=True)

    Inputs:
        - Keyword args **criteria:
            - `None` or `{}` to get all symbols
            - `exchangeName` (defaults to 'NYSE')
            - `isDelisted` (defaults to False)
            - `isSuspended` (defaults to False)
            - `index` (optional) (N/A) # TODO implement indexes like S&P 500
        - `limit`: sets a limit on the number of symbols used
        - `asset_db_name`: fully qualified path to the AssetDB
        - `stock_db_name`: fully qualified path to the Stock_DataDB
        - `update_before`: if True, updates AssetsDB upon instantiation (defaults to False)
    """
    def __init__(self, limit = None, asset_db_name='AssetDB.db', stock_db_name='Stock_DataDB.db', update_before=False, **criteria):
        self._assets = Assets(asset_db_name)
        self._main_stocks = MainStocks(stock_db_name, self._assets)
        self._extractor = DataExtractor()
        self._daily_stocks = DailyStockTables(self._main_stocks)
        
        if update_before:
            self._assets.update_all_dbs()
            # TODO Check significance of updating _main_stocks table
            self._main_stocks.repopulate_all_assets()

        if not 'isDelisted' in criteria:
            criteria['isDelisted'] = False

        if not 'isSuspended' in criteria:
            criteria['isSuspended'] = False
        
        if not 'exchangeName' in criteria:
            self._basket_of_symbols = self._assets.asset_table_manager.get_symbols_from_criteria(criteria)
            self._exchange_name = 'NYSE'
        else:
            self._exchange_name = criteria['exchangeName']
            self._basket_of_symbols = self._assets.asset_table_manager.get_symbols_from_criteria(criteria)
        
        if limit:
            if len(self._basket_of_symbols) > limit:
                self._basket_of_symbols = self._basket_of_symbols[:limit]
            else:
                warnings.warn('Limit is greater than available symbols for defined criteria')
        
        self._required_symbols_data, self._required_dates = [], []
        self.list_of_symbols = []
        

    def reset_required_vars(self):
        self._required_symbols_data, self._required_dates = [], []

    def validate_timestamps(self, start_timestamp, end_timestamp):

        if TimeHandler.get_datetime_from_string(start_timestamp) > TimeHandler.get_datetime_from_string(end_timestamp):
            raise ValueError('DateOutOfRange: start timestamp cannot be later than end timestamp')

        thisExchange = mcal.get_calendar(self._exchange_name)
        date_range = thisExchange.valid_days(TimeHandler.get_alpaca_string_from_string(start_timestamp), TimeHandler.get_alpaca_string_from_string(end_timestamp))
        new_start, new_end = TimeHandler.get_string_from_timestamp(date_range[0]), TimeHandler.get_string_from_timestamp(date_range[-1])
        if new_start != start_timestamp:
            warnings.warn(f'Start timestamp has changed from: {start_timestamp} to {new_start}')
        if new_end != end_timestamp:
            warnings.warn(f'End timestamp has changed from: {end_timestamp} to {new_end}')
        
        return new_start, new_end

    def get_stock_data(self, start_timestamp, end_timestamp, api='Alpaca'):

        start_timestamp, end_timestamp = self.validate_timestamps(start_timestamp, end_timestamp)

        for stock in self._basket_of_symbols:
            self.get_one_stock_data(stock, start_timestamp, end_timestamp)

        list_tuples, partial_list_symbols = getattr(self._extractor, f'getMultipleListHistorical{api}')(self._required_symbols_data,
                                                                                self._required_dates, TimeFrame.Day)
        # TODO len of return not same as len of list_tuples
        self._daily_stocks.update_daily_stock_data(list_tuples)
        self.reset_required_vars()
        self._extractor.AsyncObj.reset_async_list()

        self.list_of_symbols = list(set(self._basket_of_symbols).difference(set(partial_list_symbols)))
        return self._daily_stocks.get_daily_stock_data(self.list_of_symbols, start_timestamp, end_timestamp)

    def get_one_stock_data(self, stock_symbol, start_timestamp, end_timestamp):
        statusTimestamp, req_start, req_end = self._main_stocks.table_manager.check_data_availability(stock_symbol,
                                                                                                     start_timestamp,
                                                                                                     end_timestamp)

        if statusTimestamp:
            if req_start:
                self._required_symbols_data.append(stock_symbol)
                self._required_dates.append((TimeHandler.get_alpaca_string_from_string(start_timestamp),
                                            TimeHandler.get_alpaca_string_from_string(
                                                TimeHandler.get_string_from_datetime(req_start))))

            if req_end:
                self._required_symbols_data.append(stock_symbol)
                self._required_dates.append((TimeHandler.get_alpaca_string_from_string(
                    TimeHandler.get_string_from_datetime(req_end)),
                                            TimeHandler.get_alpaca_string_from_string(end_timestamp)))
        else:
            self._required_symbols_data.append(stock_symbol)
            self._required_dates.append((TimeHandler.get_alpaca_string_from_string(start_timestamp),
                                        TimeHandler.get_alpaca_string_from_string(end_timestamp)))


class MainStocks:
    def __init__(self, db_name='Stock_DataDB.db', assets: Assets = None):
        self.table_manager = MainTableManager(f'{os.path.join("tempDir", db_name)}')
        self.assets = assets

    def repopulate_all_assets(self):
        print('Updating StockData Main Database...')
        
        symbols_list = self.assets.asset_table_manager.get_all_tradable_symbols()

        for symbol in symbols_list:
            self.update_stock_symbol_main_table(symbol)
        
        print('Update completed\n')

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
    def __init__(self, main_stocks: MainStocks):
        self.main_stocks = main_stocks
        self.db = self.main_stocks.table_manager.db

    def update_daily_stock_data(self, list_of_tuples: list):
        """
        Input: list_of_tuples
        Format: [('SYMBOL1', pandas.Dataframe), ('SYMBOL2', pandas.Dataframe)...]
        """

        print('Updating DailyStockTables Database...')
        # TODO Create several DBs, slice the list_of_tuples, insert tables into each DB, 
        #      copy all the additional tables into the Main DB, delete the additional DBs.
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

        print('Update completed\n')                                                   

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
        Accepts a list of dictionaries of timestamped OHLCVTV data
        Returns: list with the new date available from and date available to
        """
        for timestamp_ohlc_dict in list_of_timestamped_data:
            if isinstance(timestamp_ohlc_dict['timestamp'], np.datetime64):
                timestamp_ohlc_dict['timestamp'] = \
                    TimeHandler.get_string_from_datetime64(timestamp_ohlc_dict['timestamp'])
            if not self.table_manager.get_one_day_data(timestamp_ohlc_dict['timestamp']):
                timestamp_ohlc_dict['volume'] = int(timestamp_ohlc_dict['volume'])
                timestamp_ohlc_dict['trade_count'] = int(timestamp_ohlc_dict['trade_count'])
                self.table_manager.insert_data(timestamp_ohlc_dict)
            else:
                warnings.warn(
                    f'Trying to insert OHLCVTC data that already exists for timestamp: {timestamp_ohlc_dict["timestamp"]}')

        return self.table_manager.get_dates_for_available_data()


if __name__ == '__main__':
    assets = Assets('AssetDB.db')
    # assets.update_db_alpaca_assets()
    main_stocks = MainStocks('Stock_DataDB.db', assets)
    main_stocks.repopulate_all_assets()

    data = DataManager(update_before=False, limit=500)
    dict_of_dfs = data.get_stock_data(TimeHandler.get_string_from_datetime(datetime(2018, 1, 1)),
                        TimeHandler.get_string_from_datetime(datetime(2018, 2, 1)))
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
