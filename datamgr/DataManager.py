import sys
import os
sys.path.insert(0, os.getcwd())  # Resolve Importing errors
from assetmgr.assetmgr_base import AssetManager
import dataset

def DataManager():
    def __init__(self):
        pass

class _MainTableManager():
    def __init__(self, db_name = 'Stock_DataDB.db'):
        self.data_DB = dataset.connect(f'sqlite:///{os.path.join("tempDir", db_name)}')
        if len(self.listTables()) == 0:
            self.create_stock_data_table()
        self.mainTable = self.data_DB.load_table(table_name='MainStockData')

        self.asset_manager = AssetManager()

    def create_stock_data_table(self):
        self.mainTable = self.data_DB.create_table(table_name='MainStockData', primary_id='stockSymbol',
                                                    primary_type=self.data_DB.types.text)
        self.mainTable.create_column('stockSymbol', self.data_DB.types.text)
        self.mainTable.create_column('dataAvailableFrom', self.data_DB.types.text)
        self.mainTable.create_column('dataAvailableTo', self.data_DB.types.text)

    def listTables(self):
        return self.data_DB.tables

    def repopulate_all_assets(self):
        symbols_list = self.asset_manager.asset_DB.returnAllTradableSymbols()
        for symbol in symbols_list:
            main_asset_data = self.return_main_asset_data(symbol)
            if main_asset_data:
                self.update_stock_symbol_main_table(symbol, main_asset_data['dataAvailableFrom'], main_asset_data['dataAvailableTo'])
            else:
                self.insert_stock_symbol_main_table(symbol)

    def return_main_asset_data(self, stockSymbol):
        return self.mainTable.find_one(stockSymbol=stockSymbol)

    def update_stock_symbol_main_table(self, stock_symbol, dataAvailableFrom="", dataAvailableTo=""):
        self.data_DB.begin()
        try:
            self.mainTable.update(
                dict(stockSymbol=stock_symbol, 
                    dataAvailableFrom=dataAvailableFrom,
                    dataAvailableTo=dataAvailableTo),
                    ['stockSymbol'])
            self.data_DB.commit()
        except Exception as exp:
            self.data_DB.rollback()
    
    def insert_stock_symbol_main_table(self, stock_symbol):
        self.data_DB.begin()
        try:
            self.mainTable.insert(
                dict(stockSymbol=stock_symbol, 
                    dataAvailableFrom="",
                    dataAvailableTo="")
                    )
            self.data_DB.commit()
        except Exception as exp:
            self.data_DB.rollback()

if __name__ == '__main__':
    data_DB = _MainTableManager()
    print(data_DB.return_main_asset_data('AAPL'))
