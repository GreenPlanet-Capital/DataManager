import os
import sys

sys.path.insert(0, os.getcwd())  # Resolve Importing errors
import dataset
from assetmgr.assetExt import AssetExtractor
from datetime import datetime, timezone
from 


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


if __name__ == '__main__':
    os.environ['SANDBOX_MODE'] = 'True'
    mgr = AssetManager('AssetDB.db')
    mgr.pullAlpacaAssets()
    a = mgr.asset_DB.returnAllAssets()
    print()
