from collections import OrderedDict
import sys
import os
import glob
sys.path.insert(0, os.getcwd())  # Resolve Importing errors
from datamgr.DataManager import _MainTableManager

for loc in glob.glob(os.path.join("tempDir",'Test*.db*')):
  os.remove(loc)

# Use Test_Stock_DataDB.db as the name (otherwise it will overwrite Stock_DataDB.db
main_table_manager = _MainTableManager('Test_Stock_DataDB.db', testmode=True)

def test_create_stock_data_table():
    assert main_table_manager.create_stock_data_table()==None

    def test_columns():
        return main_table_manager.data_DB.load_table(table_name='MainStockData').columns
    assert test_columns()==['stockSymbol', 'dataAvailableFrom', 'dataAvailableTo']

def test_listTables():
    assert main_table_manager.listTables() == ['MainStockData']

def test_insert_stock_symbol_main_table():
    main_table_manager.insert_stock_symbol_main_table('TEST_SYMBOL')
    assert return_asset_data_for_test()==OrderedDict([('stockSymbol', 'TEST_SYMBOL'), ('dataAvailableFrom', ''), ('dataAvailableTo', '')])

def return_asset_data_for_test():
    return main_table_manager.return_main_asset_data('TEST_SYMBOL')
    
def test_update_stock_symbol_main_table():
    main_table_manager.update_stock_symbol_main_table(stock_symbol='TEST_SYMBOL', dataAvailableFrom='01-01-2000', dataAvailableTo='01-01-2001')
    assert return_asset_data_for_test()==OrderedDict([('stockSymbol', 'TEST_SYMBOL'), ('dataAvailableFrom', '01-01-2000'), ('dataAvailableTo', '01-01-2001')])
    main_table_manager.update_stock_symbol_main_table(stock_symbol='TEST_SYMBOL', dataAvailableFrom='01-01-2002', dataAvailableTo='01-01-2003')
    assert return_asset_data_for_test()==OrderedDict([('stockSymbol', 'TEST_SYMBOL'), ('dataAvailableFrom', '01-01-2002'), ('dataAvailableTo', '01-01-2003')])
