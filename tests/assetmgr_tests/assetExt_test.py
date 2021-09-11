from dataset.table import Table
from assetmgr.assetmgr_base import AssetManager

# Use Test_DB.db as the name (otherwise it will override Asset_DB.db
asset_db_obj = AssetManager('Test_DB.db').asset_DB


def test_listTables():
    assert asset_db_obj.listTables() == ['Assets']


def test_listColumns():
    assert len(asset_db_obj.returnColumns()) == 6


def test_loadTable():
    assert isinstance(asset_db_obj.assetDb.load_table(table_name='Assets'), Table)


def test_columns():
    assert asset_db_obj.assetDb.load_table(table_name='Assets').columns == ['stockSymbol', 'companyName',
                                                                            'exchangeName', 'isDelisted', 'isShortable',
                                                                            'isSuspended']


def test_insertAsset():
    asset_db_obj.insertAsset('TEST_SYMBOL', 'TEST_COMPANY', 'TEST_EXCHANGE', True, False, True)
    listItems = list(asset_db_obj.returnAsset('TEST_SYMBOL').items())

    assert listItems == [('stockSymbol', 'TEST_SYMBOL'), ('companyName', 'TEST_COMPANY'),
                         ('exchangeName', 'TEST_EXCHANGE'), ('isDelisted', True), ('isShortable', False),
                         ('isSuspended', True)]


# Always use this as the last test
def test_closeTable():
    assert not asset_db_obj.close_database()
