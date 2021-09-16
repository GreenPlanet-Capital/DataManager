import os
import sys

sys.path.insert(0, os.getcwd())  # Resolve Importing errors
import dataset
from assetmgr.assetExt import AssetExtractor
from datetime import datetime, timezone


class AssetManager:
    def __init__(self, db_name='AssetDB.db'):
        self.asset_DB = _AssetDatabase(db_name)
        self.assetExtraction = AssetExtractor()

    def pullAlpacaAssets(self):
        listAlpAssets = self.assetExtraction.getAllAlpacaAssets()

        for individualAsset in listAlpAssets:
            asset_data = {'stockSymbol': individualAsset['symbol'], 'companyName': individualAsset['name'],
                          'exchangeName': individualAsset['exchange'],
                          'isDelisted': individualAsset['status'] != 'active',
                          'isShortable': individualAsset['shortable'], 'isSuspended': not individualAsset['tradable']}

            self.insert_assets_into_db(asset_data)

    def pullPyNseAssets(self):
        listPyNseAssets, _ = self.assetExtraction.getAllPyNSEAssets(threading=True)

        for individualAsset in listPyNseAssets:
            asset_data = {'stockSymbol': individualAsset['symbol'], 'companyName': individualAsset['companyName'],
                          'exchangeName': 'NSE', 'isDelisted': individualAsset['isDelisted'],
                          'isShortable': individualAsset['isSLBSec'], 'isSuspended': individualAsset['isSuspended']}

            self.insert_assets_into_db(asset_data)

    def update_iex_db(self):
        list_of_assets = self.assetExtraction.getAllIEXCloudAssets()

        for iterator in range(len(list_of_assets)):
            individualAsset = list_of_assets.iloc[iterator]
            asset_data = {'stockSymbol': individualAsset['symbol'], 'companyName': individualAsset['name'],
                          'exchangeName': individualAsset['exchange'],
                          'region': individualAsset['region'], 'currency': individualAsset['currency']}
            self.insert_assets_into_db(asset_data)

    def insert_assets_into_db(self, asset_data):
        asset_data['dateLastUpdated'] = datetime.now(timezone.utc)
        returned_Asset = self.asset_DB.returnAsset(asset_data['stockSymbol'])
        if not returned_Asset:
            self.asset_DB.insertAsset(asset_data)
        else:
            # TODO Handle for single stock having multiple exchangeNames
            self.asset_DB.updateAsset(asset_data)


class _AssetDatabase:
    def __init__(self, db_name):
        self.assetDb = dataset.connect(f'sqlite:///{os.path.join("tempDir", db_name)}')
        if len(self.listTables()) == 0:
            self.createAssetTable()
        self.assetTable = self.assetDb.load_table(table_name='Assets')

    def listTables(self):
        return self.assetDb.tables

    def close_database(self):
        self.assetDb.close()

    def createAssetTable(self):
        self.assetTable = self.assetDb.create_table('Assets', primary_id='stockSymbol',
                                                    primary_type=self.assetDb.types.text)
        self.assetTable.create_column('stockSymbol', self.assetDb.types.text)
        self.assetTable.create_column('companyName', self.assetDb.types.text)
        self.assetTable.create_column('exchangeName', self.assetDb.types.text)
        self.assetTable.create_column('dateLastUpdated', self.assetDb.types.datetime)
        self.assetTable.create_column('region', self.assetDb.types.text)
        self.assetTable.create_column('currency', self.assetDb.types.text)
        self.assetTable.create_column('isDelisted', self.assetDb.types.boolean)
        self.assetTable.create_column('isShortable', self.assetDb.types.boolean)
        self.assetTable.create_column('isSuspended', self.assetDb.types.boolean)

    def insertAsset(self, asset_data):
        self.assetDb.begin()
        try:
            self.assetTable.insert(asset_data)
            self.assetDb.commit()
        except Exception as exp:
            self.assetDb.rollback()

    def updateAsset(self, asset_data):
        self.assetDb.begin()
        try:
            self.assetTable.update(asset_data, ['stockSymbol'])
            self.assetDb.commit()
        except Exception as exp:
            self.assetDb.rollback()

    def returnAllAssets(self):
        return list(self.assetTable.all())

    def returnAsset(self, stockSymbol):
        return self.assetTable.find_one(stockSymbol=stockSymbol)

    def returnExchangeBucket(self, exchangeName, isDelisted=False, isSuspended=False):
        return list(self.assetTable.find(exchangeName=exchangeName, isDelisted=isDelisted, isSuspended=isSuspended))

    def returnAllTradableSymbols(self, isDelisted=False, isSuspended=False):
        return [row['stockSymbol'] for row in self.assetTable.find(isDelisted=isDelisted, isSuspended=isSuspended)]

    def returnColumns(self):
        return self.assetTable.columns


if __name__ == '__main__':
    os.environ['SANDBOX_MODE'] = 'True'
    mgr = AssetManager('AssetDB.db')
    mgr.pullAlpacaAssets()
    a = mgr.asset_DB.returnAllAssets()
    print()
