import sys
import os
sys.path.insert(0, os.getcwd())  # Resolve Importing errors
import requests
import configparser
from pynse import Nse
import concurrent.futures
from assetmgr.nseList import listNSESymbols
from iexfinance import refdata


class AssetExtractor:
    def __init__(self):
        self.configParse = configparser.ConfigParser()
        self.configParse.read(os.path.join('config_files', 'assetConfig.cfg'))
        self.NSEApi = Nse()
        self.AlpacaHeaders = {
            'APCA-API-KEY-ID': self.configParse.get('Alpaca', 'AlpacaKey'),
            'APCA-API-SECRET-KEY': self.configParse.get('Alpaca', 'AlpacaSecret'),
        }
        self.IEXAuthSandbox = {
            'PublicKey': self.configParse.get('IEX_Sandbox', 'IEX_Sandbox_Public'),
            'PrivateKey': self.configParse.get('IEX_Sandbox', 'IEX_Sandbox_Private'),
        }
        self.IEXAuth = {
            'PublicKey': self.configParse.get('IEX_Real', 'IEX_Public'),
            'PrivateKey': self.configParse.get('IEX_Real', 'IEX_Private'),
        }
        os.environ["IEX_TOKEN"] = self.IEXAuth['PrivateKey']
        # os.environ["IEX_API_VERSION"] = "iexcloud-sandbox"

    def getAllIEXCloudAssets(self):
        return refdata.get_symbols()

    def getAlpacaAsset(self, stockSymbol):
        response = requests.get(f'https://paper-api.alpaca.markets/v2/assets/{stockSymbol}', headers=self.AlpacaHeaders)
        return response.json()

    def getAllAlpacaAssets(self):
        response = requests.get('https://paper-api.alpaca.markets/v2/assets', headers=self.AlpacaHeaders)
        return response.json()

    def getPyNSEAsset(self, stockSymbol):
        try:
            to_return = self.NSEApi.info(stockSymbol)
        except ValueError as e:
            return ['Symbol not found', stockSymbol]
        return to_return

    def getAllPyNSEAssets(self, threading=True):
        if threading:
            executor = concurrent.futures.ProcessPoolExecutor(10)
            listNSEAssets = [executor.submit(self.getPyNSEAsset, stockSymbol) for stockSymbol in listNSESymbols]
            concurrent.futures.wait(listNSEAssets)
        else:
            listNSEAssets = [self.getPyNSEAsset(stockSymbol) for stockSymbol in listNSESymbols]

        symbols_not_found = []
        positions_to_be_removed = set()

        def process_Futures_objects():
            for i, futures_object in enumerate(listNSEAssets):
                result = futures_object.result()
                if isinstance(result, dict):
                    listNSEAssets[i] = result
                elif result[0] == 'Symbol not found':
                    symbols_not_found.append(result[1])
                    positions_to_be_removed.add(i)

        process_Futures_objects()
        listNSEAssets = [asset for i, asset in enumerate(listNSEAssets) if i not in positions_to_be_removed]
        return listNSEAssets, symbols_not_found

if __name__ == '__main__':
    extractor = AssetExtractor()
    a = extractor.getAllIEXCloudAssets()
    print()