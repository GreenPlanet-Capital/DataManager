from database_layer.database import DatabaseManager
from datetime import datetime, timezone
import sys
import os

sys.path.insert(0, os.getcwd())  # Resolve Importing errors
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
            main_asset_data = self.table_manager.get_one_asset(symbol)
            if main_asset_data:
                self.update_stock_symbol_main_table(symbol, main_asset_data['dataAvailableFrom'],
                                                    main_asset_data['dataAvailableTo'])
            else:
                self.insert_stock_symbol_main_table(symbol)

    def update_stock_symbol_main_table(self, stockSymbol, dataAvailableFrom=None, dataAvailableTo=None):
        asset_data = {'stockSymbol': stockSymbol,
                'dataAvailableFrom': dataAvailableFrom,
                'dataAvailableTo': dataAvailableTo,
                'dateLastUpdated': datetime.now(timezone.utc)
                }
        self.table_manager.update_asset(asset_data)

    def insert_stock_symbol_main_table(self, stockSymbol, dataAvailableFrom=None, dataAvailableTo=None):
        asset_data = {'stockSymbol': stockSymbol,
                'dataAvailableFrom': dataAvailableFrom,
                'dataAvailableTo': dataAvailableTo,
                'dateLastUpdated': datetime.now(timezone.utc)
                }
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
            self.table_manager.insert_data(timestamp_ohlc_dict)

        return self.table_manager.get_dates_for_available_data()


if __name__ == '__main__':
    assets = Assets('AssetDB.db')
    main_stocks = MainStocks('Stock_DataDB.db', assets)
    main_stocks.repopulate_all_assets()

    print()