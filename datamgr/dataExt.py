from datetime import datetime
import sys
import os
sys.path.insert(0, os.getcwd())  # Resolve Importing errors
import requests
import configparser
from pynse import Nse
import concurrent.futures
from assetmgr.nseList import listNSESymbols
from iexfinance import stocks
from assetmgr.assetmgr_base import AssetManager

class DataExtractor():
    def __init__(self, sandbox_mode=True) -> None:
        self.configParse = configparser.ConfigParser()
        self.configParse.read(os.path.join('config_files', 'assetConfig.cfg'))
        self.IEXAuthSandbox = {
            'PublicKey': self.configParse.get('IEX_Sandbox', 'IEX_Sandbox_Public'),
            'PrivateKey': self.configParse.get('IEX_Sandbox', 'IEX_Sandbox_Private'),
        }
        self.IEXAuth = {
            'PublicKey': self.configParse.get('IEX_Real', 'IEX_Public'),
            'PrivateKey': self.configParse.get('IEX_Real', 'IEX_Private'),
        }
        if sandbox_mode:
            os.environ["IEX_TOKEN"] = self.IEXAuthSandbox['PrivateKey']
            os.environ["IEX_API_VERSION"] = "iexcloud-sandbox"
        else:
            os.environ["IEX_TOKEN"] = self.IEXAuth['PrivateKey']
    
    def extract_iex_historical(self, list_of_symbols, start: datetime, end: datetime, close_only=False, output_format='pandas'):
        dict_of_dataframes = {}
        not_found_symbols = []
        for slice in range(100,len(list_of_symbols),100):       #TODO: Thread this
            df = stocks.get_historical_data(list_of_symbols[slice-100:slice], start=start, end=end, close_only=close_only, output_format=output_format)
            for symbol in list_of_symbols:
                try:
                    dict_of_dataframes[symbol] = df.loc[:, symbol]
                except KeyError:
                    not_found_symbols.append(symbol)           
        return dict_of_dataframes, not_found_symbols




if __name__ == '__main__':
    extractor = DataExtractor(sandbox_mode=True)
    manager = AssetManager(sandbox_mode=True)
    manager.pullAlpacaAssets()
    output, _ = extractor.extract_iex_historical(manager.asset_DB.returnAllTradableSymbols()[:500], datetime(2017, 2, 9), datetime(2017, 5, 24))
    print()
