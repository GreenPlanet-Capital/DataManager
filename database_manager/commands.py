import sys
import os
sys.path.insert(0, os.getcwd())  # Resolve Importing errors
from database_manager.database import DatabaseManager

class AssetTable:

    def __init__(self, db_name):
        self.db = DatabaseManager(db_name)
        self.table_name = 'Assets'
        self.columns = {
                        'stockSymbol': 'text not null primary key',
                        'companyName': 'text not null',
                        'exchangeName': 'text not null',
                        'dateLastUpdated': 'text not null',
                        'region': 'text',
                        'currency': 'text',
                        'isDelisted': 'integer not null',
                        'isShortable': 'integer not null',
                        'isSuspended': 'integer not null',
                                        }
        self.table = self.create_asset_table(self.table_name, self.columns)

    def __del__(self):
        del(self.db)

    def list_tables(self):
        return [e for (e,) in self.db.list_tables().fetchall()]

    def create_asset_table(self, table_name, columns):
        self.db.create_table(f'{table_name}', columns)

    def insert_asset(self, asset_data):
        self.db.add(self.table_name, asset_data)

    def update_asset(self, asset_data):
        self.db.update(self.table_name, {'stockSymbol': asset_data['stockSymbol']}, asset_data)

    def get_assets_list(self):
        list_of_assets = self.db.select(self.table_name).fetchall()
        return tuples_to_dict(list_of_assets, self.columns)
    
    def get_one_asset(self, stock_symbol):
        asset = self.db.select(self.table_name, {'stockSymbol': stock_symbol}).fetchone()
        return asset_row_to_dict(self.columns, asset)

    def get_exchange_basket(self, exchangeName, isDelisted=False, isSuspended=False):
        list_of_assets = self.db.select(self.table_name, {'exchangeName': exchangeName, 
                                                        'isDelisted': isDelisted, 
                                                        'isSuspended': isSuspended}).fetchall()
        return tuples_to_dict(list_of_assets, self.columns)

    def get_all_tradable_symbols(self, isDelisted=False, isSuspended=False):
        list_of_assets = self.db.select(self.table_name, {'isDelisted': isDelisted, 
                                                        'isSuspended': isSuspended}).fetchall()
        return tuples_to_dict(list_of_assets, self.columns)

    def get_columns(self):
        return self.columns

def tuples_to_dict(list_of_asset_tuples, columns_dict):
        return [asset_row_to_dict(columns_dict, row) for row in list_of_asset_tuples]

def asset_row_to_dict(columns_dict, row):
        return dict(zip(columns_dict.keys(), row))
    

if '__main__'==__name__:
    asset_table = AssetTable(f'{os.path.join("tempDir", "Asset_Test.db")}')
    data = {
            'stockSymbol': 'TEST_SYMBOL_TWO',
            'companyName': 'TEST_COMPANY',
            'exchangeName': 'TEST_EXCHANGE',
            'dateLastUpdated': '2020-01-01',
            'region': 'TomorrowLand',
            'currency': 'USD',
            'isDelisted': 0,
            'isShortable': 1,
            'isSuspended': 0,
            }
    # asset_table.insert_asset(data)
    # asset_table.update_asset(data)
    # output = asset_table.get_exchange_basket(exchangeName='TEST_EXCHANGE')
    # output = asset_table.get_assets_list()
    output = asset_table.get_all_tradable_symbols()
    print()