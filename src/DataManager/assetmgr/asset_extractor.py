from alpaca_trade_api.rest import REST
from DataManager import core


class AssetExtractor:
    def __init__(self):
        core.setEnv()
        self.AlpacaAPI = REST(raw_data=True)
        self.NSEApi = None

    def getAlpacaAsset(self, stockSymbol):
        return self.AlpacaAPI.get_asset(stockSymbol)

    def getAllAlpacaAssets(self):
        return self.AlpacaAPI.list_assets()


if __name__ == "__main__":
    extractor = AssetExtractor()
    listA = extractor.getAllAlpacaAssets()
