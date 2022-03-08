import concurrent.futures
from typing import Any, List
from DataManager.assetmgr.nse_list import listNSESymbols
from iexfinance import refdata
from alpaca_trade_api.rest import REST
from DataManager import core
import os


class AssetExtractor:
    def __init__(self):
        core.setEnv()
        self.AlpacaAPI = REST(raw_data=True)
        self.NSEApi = None

    def getAllIEXCloudAssets(self):
        return refdata.get_symbols()

    def getAlpacaAsset(self, stockSymbol):
        return self.AlpacaAPI.get_asset(stockSymbol)

    def getAllAlpacaAssets(self):
        return self.AlpacaAPI.list_assets()

    def getPyNSEAsset(self, stockSymbol):
        """
        Does not work
        """
        # try:
        #     to_return = self.NSEApi.info(stockSymbol)
        # except ValueError as e:
        #     return ['Symbol not found', stockSymbol]
        # return to_return
        pass

    def getAllPyNSEAssets(self, threading=True):
        listNSEAssets: List[Any] = []
        if threading:
            executor = concurrent.futures.ProcessPoolExecutor(10)
            listNSEAssets = [
                executor.submit(self.getPyNSEAsset, stockSymbol)
                for stockSymbol in listNSESymbols
            ]
            concurrent.futures.wait(listNSEAssets)
        else:
            listNSEAssets = [
                self.getPyNSEAsset(stockSymbol) for stockSymbol in listNSESymbols
            ]

        symbols_not_found: List[str] = []
        positions_to_be_removed = set()

        def process_Futures_objects():
            for i, futures_object in enumerate(listNSEAssets):
                result: Any = futures_object.result()
                if isinstance(result, dict):
                    listNSEAssets[i] = result
                elif result[0] == "Symbol not found":
                    symbols_not_found.append(result[1])
                    positions_to_be_removed.add(i)

        process_Futures_objects()
        listNSEAssets = [
            asset
            for i, asset in enumerate(listNSEAssets)
            if i not in positions_to_be_removed
        ]
        return listNSEAssets, symbols_not_found


if __name__ == "__main__":
    os.environ["SANDBOX_MODE"] = "True"
    extractor = AssetExtractor()
    listA = extractor.getAllAlpacaAssets()
