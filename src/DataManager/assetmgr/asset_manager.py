import os
from DataManager.assetmgr.asset_extractor import AssetExtractor
from datetime import datetime, timezone
from DataManager.database_layer.tables import AssetTableManager
from DataManager.utils.timehandler import TimeHandler
from DataManager.core import setEnv, DATAMGR_ABS_PATH
from pandas import read_html


class Assets:
    def __init__(self, db_name="AssetDB.db"):
        setEnv()
        self.asset_table_manager = AssetTableManager(
            os.path.join(DATAMGR_ABS_PATH, os.path.join("tempDir", db_name))
        )
        self.assetExtraction = AssetExtractor()

    def update_db_alpaca_assets(self):
        listAlpAssets = self.assetExtraction.getAllAlpacaAssets()
        for individualAsset in listAlpAssets:
            asset_data = {
                "stockSymbol": individualAsset["symbol"],
                "companyName": individualAsset["name"],
                "exchangeName": individualAsset["exchange"],
                "isDelisted": individualAsset["status"] != "active",
                "isShortable": individualAsset["shortable"],
                "isSuspended": not individualAsset["tradable"],
            }

            self.insert_assets_into_db(asset_data)

    def update_index_assets(self):

        # Reset all index values
        list_all_symbols = self.asset_table_manager.get_symbols_from_criteria(None)
        for each_symbol in list_all_symbols:
            self.asset_table_manager.update_asset(
                {"stockSymbol": each_symbol, "index_name": None}
            )

        dict_index_stocks = {
            "snp": read_html(
                "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
            )[0]["Symbol"].values.tolist()
        }

        for index_name, list_stocks in dict_index_stocks.items():
            for one_stock in list_stocks:
                if self.asset_table_manager.get_one_asset(one_stock):
                    self.asset_table_manager.update_asset(
                        {"stockSymbol": one_stock, "index_name": index_name}
                    )

    def update_all_dbs(self):
        print("Updating Assets Database...")
        list_of_update_methods = [
            method for method in dir(self) if "update_db_" in method
        ]
        for update_method in list_of_update_methods:
            getattr(self, update_method)()
        print("Updating index columns")
        self.update_index_assets()
        print("Update completed\n")

    def insert_assets_into_db(self, asset_data):
        asset_data["dateLastUpdated"] = TimeHandler.get_string_from_datetime(
            datetime.now(timezone.utc)
        )
        returned_Asset = self.asset_table_manager.get_one_asset(
            asset_data["stockSymbol"]
        )
        if not returned_Asset:
            self.asset_table_manager.insert_asset(asset_data)
        else:
            # TODO Handle for single stock having multiple exchangeNames
            self.asset_table_manager.update_asset(asset_data)


if __name__ == "__main__":
    mgr = Assets("AssetDB.db")
    mgr.update_db_alpaca_assets()
    a = mgr.asset_table_manager.get_assets_list()
    print()
