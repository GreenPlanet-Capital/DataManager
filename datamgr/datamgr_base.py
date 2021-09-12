import sys
import os
import dataset

sys.path.insert(0, os.getcwd())  # Resolve Importing errors
from assetmgr.assetmgr_base import AssetManager


def DataManager():
    def __init__(self):
        pass


class _MainTableManager:
    def __init__(self, db_name='Stock_DataDB.db', testmode=False):
        self.db_connection = dataset.connect(f'sqlite:///{os.path.join("tempDir", db_name)}')
        if len(self.listTables()) == 0:
            self.create_stock_data_table()
        self.mainTable = self.db_connection.load_table(table_name='MainStockData')

        if testmode:
            self.asset_manager = AssetManager('Test_Stock_DataDB.db')
        else:
            self.asset_manager = AssetManager()

    def create_stock_data_table(self):
        self.mainTable = self.db_connection.create_table(table_name='MainStockData', primary_id='stockSymbol',
                                                         primary_type=self.db_connection.types.text)
        self.mainTable.create_column('stockSymbol', self.db_connection.types.text)
        self.mainTable.create_column('dataAvailableFrom', self.db_connection.types.datetime)
        self.mainTable.create_column('dataAvailableTo', self.db_connection.types.datetime)

    def listTables(self):
        return self.db_connection.tables

    def repopulate_all_assets(self):
        symbols_list = self.asset_manager.asset_DB.returnAllTradableSymbols()
        for symbol in symbols_list:
            main_asset_data = self.return_main_asset_data(symbol)
            if main_asset_data:
                self.update_stock_symbol_main_table(symbol, main_asset_data['dataAvailableFrom'],
                                                    main_asset_data['dataAvailableTo'])
            else:
                self.insert_stock_symbol_main_table(symbol)

    def return_main_asset_data(self, stockSymbol):
        return self.mainTable.find_one(stockSymbol=stockSymbol)

    def update_stock_symbol_main_table(self, stock_symbol, dataAvailableFrom=None, dataAvailableTo=None):
        self.db_connection.begin()
        try:
            self.mainTable.update(
                dict(stockSymbol=stock_symbol,
                     dataAvailableFrom=dataAvailableFrom,
                     dataAvailableTo=dataAvailableTo),
                ['stockSymbol'])
            self.db_connection.commit()
        except Exception as exp:
            self.db_connection.rollback()

    def insert_stock_symbol_main_table(self, stock_symbol):
        self.db_connection.begin()
        try:
            self.mainTable.insert(
                dict(stockSymbol=stock_symbol,
                     dataAvailableFrom="",
                     dataAvailableTo="")
            )
            self.db_connection.commit()
        except Exception as exp:
            self.db_connection.rollback()


class _SubTableManager:
    def __init__(self, db_name='Stock_DataDB.db', testmode=False):
        self.db_connection = dataset.connect(f'sqlite:///{os.path.join("tempDir", db_name)}')


class _SubTable:
    pass


if __name__ == '__main__':
    db_connection = _MainTableManager()
    print(db_connection.return_main_asset_data('AAPL'))
