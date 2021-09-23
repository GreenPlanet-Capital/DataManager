from datetime import timedelta
from sqlite3.dbapi2 import Time
import sys
import os

sys.path.insert(0, os.getcwd())  # Resolve Importing errors
from database_layer.database import DatabaseManager
from utils.conversions import _Conversions
from utils.timehandler import TimeHandler


class TableManager:
    def __init__(self, db_name):
        self.db = DatabaseManager(db_name)
        self.table_name = ''
        self.columns = {}

    def __del__(self):
        del (self.db)

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
        return _Conversions.tuples_to_dict(list_of_assets, self.columns)

    def get_one_asset(self, stock_symbol):
        asset = self.db.select(self.table_name, {'stockSymbol': stock_symbol}).fetchone()
        return _Conversions.asset_row_to_dict(self.columns, asset) if asset else None

    def get_columns(self):
        return self.columns


class AssetTableManager(TableManager):

    def __init__(self, db_name):
        super().__init__(db_name)
        self.table_name = 'Assets'
        self.columns = {
            'stockSymbol': 'text not null primary key',
            'companyName': 'text not null',
            'exchangeName': 'text not null',
            'dateLastUpdated': 'text not null',
            'region': 'text',
            'currency': 'text',
            'isDelisted': 'integer',
            'isShortable': 'integer',
            'isSuspended': 'integer',
        }
        self.create_asset_table(self.table_name, self.columns)

    def get_exchange_basket(self, exchangeName, isDelisted=False, isSuspended=False):
        list_of_assets = self.db.select(self.table_name, {'exchangeName': exchangeName,
                                                          'isDelisted': isDelisted,
                                                          'isSuspended': isSuspended}).fetchall()
        return _Conversions.tuples_to_dict(list_of_assets, self.columns)

    def get_all_tradable_symbols(self, isDelisted=False, isSuspended=False):
        list_of_assets = self.db.select(self.table_name, {'isDelisted': isDelisted,
                                                          'isSuspended': isSuspended}).fetchall()
        return [asset['stockSymbol'] for asset in _Conversions.tuples_to_dict(list_of_assets, self.columns)]

    def get_symbols_from_criteria(self, criteria):
        list_of_assets = self.db.select(self.table_name, criteria).fetchall()
        return [asset['stockSymbol'] for asset in _Conversions.tuples_to_dict(list_of_assets, self.columns)]


class MainTableManager(TableManager):

    def __init__(self, db_name):
        self.db = DatabaseManager(db_name)
        self.table_name = 'MainStockData'
        self.columns = {
            'stockSymbol': 'text not null primary key',
            'dataAvailableFrom': 'text',
            'dataAvailableTo': 'text',
            'dateLastUpdated': 'text not null',
        }
        self.create_asset_table(self.table_name, self.columns)

    def check_data_availability(self, stock_symbol, start_timestamp, end_timestamp):
        start_timestamp = TimeHandler.get_datetime_from_string(start_timestamp)
        end_timestamp = TimeHandler.get_datetime_from_string(end_timestamp)

        fetchDateStart = self.db.select_column_value(self.table_name, stock_symbol, 'dataAvailableFrom').fetchall()[0][0]
        fetchDateEnd = self.db.select_column_value(self.table_name, stock_symbol, 'dataAvailableTo').fetchall()[0][0]

        if not fetchDateStart or not fetchDateEnd:
            return False, start_timestamp, end_timestamp

        dataAvailableFrom = TimeHandler.get_datetime_from_string(fetchDateStart)
        dataAvailableTo = TimeHandler.get_datetime_from_string(fetchDateEnd)

        start_time_delta = dataAvailableFrom - start_timestamp
        end_time_delta = end_timestamp - dataAvailableTo

        required_start_timestamp, required_end_timestamp  = None, None

        if start_time_delta.days>0:
            required_start_timestamp = dataAvailableFrom - timedelta(days=1)
        if end_time_delta.days>0:
            required_end_timestamp = dataAvailableTo + timedelta(days=1)

        return True, required_start_timestamp, required_end_timestamp

class DailyDataTableManager:

    def __init__(self, table_name, db: DatabaseManager):
        self.db = db
        self.table_name = table_name
        self.columns = {
            'timestamp': 'text not null primary key',
            'open': 'real',
            'high': 'real',
            'low': 'real',
            'close': 'real',
            'volume': 'integer',
            'trade_count': 'integer',
            'vwap': 'real',
        }
        self.create_sub_table(self.table_name, self.columns)

    def __del__(self):
        del (self.db)

    def create_sub_table(self, table_name, columns):
        self.db.create_table(f'{table_name}', columns)

    def insert_data(self, asset_data):
        self.db.add(self.table_name, asset_data)

    def get_one_day_data(self, timestamp):
        asset = self.db.select(self.table_name, {'timestamp': timestamp}).fetchone()
        return _Conversions.asset_row_to_dict(self.columns, asset) if asset else None

    def get_data(self, start_timestamp, end_timestamp):
        criteria = {'start_timestamp': start_timestamp,
                    'end_timestamp': end_timestamp}
        order_by = 'timestamp'
        daily_data = self.db.select_between_range(self.table_name, criteria, order_by).fetchall()
        return _Conversions().tuples_to_dict(daily_data, self.columns)

    def get_dates_for_available_data(self):
        def get_date_available_from(self: DailyDataTableManager):
            return self.db.select_min_value_from_column(self.table_name, 'timestamp').fetchall()

        def get_date_available_to(self: DailyDataTableManager):
            return self.db.select_max_value_from_column(self.table_name, 'timestamp').fetchall()

        return get_date_available_from(self)[0][0], get_date_available_to(self)[0][0]

    def _update_one_day_data(self, timestamp_ohlc_dict):
        self.db.update(self.table_name, {'timestamp': timestamp_ohlc_dict['timestamp']}, timestamp_ohlc_dict)


if '__main__' == __name__:
    asset_table = AssetTableManager(f'{os.path.join("tempDir", "Asset_Test.db")}')
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
    asset_table.insert_asset(data)
    # asset_table.update_asset(data)
    output = asset_table.get_exchange_basket(exchangeName='TEST_EXCHANGE')
    output = asset_table.get_assets_list()
    # output = asset_table.get_all_tradable_symbols()

    db = DatabaseManager(f'{os.path.join("tempDir", "Stock_DataDB.db")}')
    daily_data_table = DailyDataTableManager('TEST_SYMBOL_TABLE', db)
    a = daily_data_table.get_data('2021-09-12', '2021-09-20')
    print()
