import os
import sys
sys.path.insert(0, os.getcwd())  # Resolve Importing errors
import dataset
from assetmgr.assetExt import AssetExtractor

class AssetManager:
    def __init__(self, db_name='AssetDB.db'):
        self.thisDB = _AssetDatabase(db_name)
        self.assetExtraction = AssetExtractor()

    def pullAlpacaAssets(self):
        listAlpAssets = self.assetExtraction.getAllAlpacaAssets()

        for individualAsset in listAlpAssets:
            assetDb = self.thisDB.returnAsset(individualAsset['symbol'])
            listParameters = (individualAsset['symbol'], individualAsset['name'],
                              individualAsset['exchange'], individualAsset['status'] != 'active',
                              individualAsset['shortable'], not individualAsset['tradable'])

            if not assetDb:
                self.thisDB.insertAsset(*listParameters)
            else:
                self.thisDB.updateAsset(*listParameters)

    def pullPyNseAssets(self):
        listPyNseAssets, _ = self.assetExtraction.getAllPyNSEAssets(threading=True)

        for individualAsset in listPyNseAssets:
            assetDb = self.thisDB.returnAsset(individualAsset['symbol'])
            listParameters = (individualAsset['symbol'], individualAsset['companyName'],
                              'NSE', individualAsset['isDelisted'],
                              individualAsset['isSLBSec'], individualAsset['isSuspended'])

            if not assetDb:
                self.thisDB.insertAsset(*listParameters)
            else:
                self.thisDB.updateAsset(*listParameters)

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

    def begin_transaction(self):  # can be removed
        self.assetDb.begin()

    def commit_transaction(self):  # can be removed
        self.assetDb.commit()

    def rollback_transaction(self):
        self.assetDb.rollback()

    def createAssetTable(self):
        self.assetTable = self.assetDb.create_table('Assets', primary_id='stockSymbol',
                                                    primary_type=self.assetDb.types.text)
        self.assetTable.create_column('stockSymbol', self.assetDb.types.text)
        self.assetTable.create_column('companyName', self.assetDb.types.text)
        self.assetTable.create_column('exchangeName', self.assetDb.types.text)
        self.assetTable.create_column('isDelisted', self.assetDb.types.boolean)
        self.assetTable.create_column('isShortable', self.assetDb.types.boolean)
        self.assetTable.create_column('isSuspended', self.assetDb.types.boolean)

    def insertAsset(self, stockSymbol, companyName, exchangeName, isDelisted, isShortable, isSuspended):
        self.assetDb.begin()
        try:
            self.assetTable.insert(
                dict(stockSymbol=stockSymbol, companyName=companyName, exchangeName=exchangeName,
                     isDelisted=isDelisted, isShortable=isShortable, isSuspended=isSuspended))
            self.assetDb.commit()
        except Exception as exp:
            self.rollback_transaction()

    def updateAsset(self, stockSymbol, companyName, exchangeName, isDelisted, isShortable, isSuspended):
        self.assetDb.begin()
        try:
            self.assetTable.update(
                dict(stockSymbol=stockSymbol, companyName=companyName, exchangeName=exchangeName,
                     isDelisted=isDelisted, isShortable=isShortable, isSuspended=isSuspended), ['stockSymbol'])
            self.assetDb.commit()
        except Exception as exp:
            self.rollback_transaction()

    def returnAllAssets(self):
        return list(self.assetTable.all())

    def returnAsset(self, stockSymbol):
        return self.assetTable.find_one(stockSymbol=stockSymbol)

    def returnExchangeBucket(self, exchangeName, isDelisted=False, isSuspended=False):
        return list(self.assetTable.find(exchangeName=exchangeName, isDelisted=isDelisted, isSuspended=isSuspended))

    def returnColumns(self):
        return self.assetTable.columns


if __name__ == '__main__':
    mgr = AssetManager()
    mgr.pullAlpacaAssets()
    mgr.pullPyNseAssets()
