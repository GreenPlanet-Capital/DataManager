from datetime import datetime, timezone
import sys
import os
import warnings

sys.path.insert(0, os.getcwd())  # Resolve Importing errors
from database_layer.database import DatabaseManager
from assetmgr.asset_manager import Assets
from database_layer.tables import DailyDataTableManager, MainTableManager

def DataManager():
    def __init__(self):
        pass


class MainStocks:
    def __init__(self, db_name = 'Stock_DataDB.db', assets: Assets=None):
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
                'dateLastUpdated': datetime.now(timezone.utc)
                }
        main_asset_data = self.table_manager.get_one_asset(stockSymbol)
        if main_asset_data:
            asset_data['dataAvailableFrom'] = dataAvailableFrom if dataAvailableFrom is not None else main_asset_data['dataAvailableFrom']
            asset_data['dataAvailableTo'] = dataAvailableTo if dataAvailableTo is not None else main_asset_data['dataAvailableTo']
            self.table_manager.update_asset(asset_data)
        else:
            self.table_manager.insert_asset(asset_data)


class DailyStockDataTable:
    def __init__(self, table_name, db: DatabaseManager):
        self.table_manager = DailyDataTableManager(table_name=table_name, db=db)

    def update_daily_stock_data(self, list_of_timestamped_data: list):
        """
        Accepts a list of dictionaries of timestamped OHLCVTV data
        Returns: tuple with the new date available from and date available to
        """
        for timestamp_ohlc_dict in list_of_timestamped_data:
            if not self.table_manager.get_one_day_data(timestamp_ohlc_dict['timestamp']):
                self.table_manager.insert_data(timestamp_ohlc_dict)
            else:
                warnings.warn('Trying to insert OHLCVTC data which already exists for given timestamp')

        return self.table_manager.get_dates_for_available_data()


if __name__ == '__main__':
    # assets = Assets('AssetDB.db')
    # main_stocks = MainStocks('Stock_DataDB.db', assets)
    # main_stocks.repopulate_all_assets()
    db_manager = DatabaseManager(f'{os.path.join("tempDir", "Stock_DataDB_Test.db")}')
    test_symbol_stock_table = DailyStockDataTable('TEST_SYMBOL_TWO', db_manager)
    main_stocks = MainStocks('Stock_DataDB_Test.db')


    list_of_timestamped_data = ({'timestamp': '2021-01-01',
                                'open': 150.5655,
                                'high': 155.5655,
                                'low': 145.5909,
                                'close': 148.5390,
                                'volume': 2003940,
                                'trade_count': 45000,
                                'vwap': None,}, 
                                {'timestamp': '2021-01-02',
                                'open': 152.5655,
                                'high': 157.5655,
                                'low': 147.5909,
                                'close': 150.5390,
                                'volume': 345_000,
                                'trade_count': 50_000,
                                'vwap': None,}, 
                                {'timestamp': '2021-01-03',
                                'open': 154.5655,
                                'high': 159.5655,
                                'low': 149.5909,
                                'close': 151.5390,
                                'volume': 100_000,
                                'trade_count': 35_000,
                                'vwap': None,}, 
                                )
    dateAvailableFrom, dateAvailableTo = test_symbol_stock_table.update_daily_stock_data(list_of_timestamped_data=list_of_timestamped_data)
    main_stocks.update_stock_symbol_main_table(test_symbol_stock_table.table_manager.table_name, dataAvailableFrom=dateAvailableFrom, dataAvailableTo=dateAvailableTo)

    print()