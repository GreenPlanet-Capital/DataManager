import os
import sys
sys.path.insert(0, os.getcwd())  # Resolve Importing errors=
from assetmgr.asset_extractor import AssetExtractor
from datetime import datetime, timezone
from database_layer.tables import AssetTableManager
from utils.timehandler import TimeHandler
from core import setEnv


class Assets:
    def __init__(self, db_name='AssetDB.db'):
        setEnv()
        self.asset_table_manager = AssetTableManager(os.path.join("tempDir", db_name))
        self.assetExtraction = AssetExtractor()

    def update_db_alpaca_assets(self):
        listAlpAssets = self.assetExtraction.getAllAlpacaAssets()

        for individualAsset in listAlpAssets:
            asset_data = {'stockSymbol': individualAsset['symbol'], 'companyName': individualAsset['name'],
                          'exchangeName': individualAsset['exchange'],
                          'isDelisted': individualAsset['status'] != 'active',
                          'isShortable': individualAsset['shortable'], 'isSuspended': not individualAsset['tradable']}

            self.insert_assets_into_db(asset_data)

    # def update_db_pynse_assets(self):
    #     listPyNseAssets, _ = self.assetExtraction.getAllPyNSEAssets(threading=True)

    #     for individualAsset in listPyNseAssets:
    #         asset_data = {'stockSymbol': individualAsset['symbol'], 'companyName': individualAsset['companyName'],
    #                       'exchangeName': 'NSE', 'isDelisted': individualAsset['isDelisted'],
    #                       'isShortable': individualAsset['isSLBSec'], 'isSuspended': individualAsset['isSuspended']}

    #         self.insert_assets_into_db(asset_data)

    def update_db_iex_assets(self):
        list_of_assets = self.assetExtraction.getAllIEXCloudAssets()

        for iterator in range(len(list_of_assets)):
            individualAsset = list_of_assets[iterator]
            asset_data = {'stockSymbol': individualAsset['symbol'], 'companyName': individualAsset['name'],
                          'exchangeName': individualAsset['exchange'],
                          'region': individualAsset['region'], 'currency': individualAsset['currency']}
            self.insert_assets_into_db(asset_data)

    def update_all_dbs(self):
        list_of_update_methods = [method for method in dir(self) if "update_db_" in method]
        for update_method in list_of_update_methods:
            getattr(self, update_method)()


    def insert_assets_into_db(self, asset_data):
        # TODO Ensure that at the end of an updation cycle that no isDelisted=Null, isShortable=Null exist
        asset_data['dateLastUpdated'] = TimeHandler.get_string_from_datetime(datetime.now(timezone.utc))
        returned_Asset = self.asset_table_manager.get_one_asset(asset_data['stockSymbol'])
        if not returned_Asset:
            self.asset_table_manager.insert_asset(asset_data)
        else:
            # TODO Handle for single stock having multiple exchangeNames
            self.asset_table_manager.update_asset(asset_data)


if __name__ == '__main__':
    os.environ['SANDBOX_MODE'] = 'True'
    mgr = Assets('AssetDB.db')
    mgr.update_db_alpaca_assets()
    mgr.update_db_iex_assets()
    a = mgr.asset_table_manager.get_assets_list()
    print()
