import configparser
import os
import inspect
import DataManager

DATAMGR_ABS_PATH = os.path.dirname(inspect.getfile(DataManager))

configParse = configparser.ConfigParser()
configParse.read(
    os.path.join(DATAMGR_ABS_PATH, os.path.join("config_files", "assetConfig.cfg"))
)
AlpacaAuth = dict(configParse["Alpaca"])


def setEnv():
    os.environ["APCA_API_BASE_URL"] = "https://paper-api.alpaca.markets/"
    os.environ["APCA_API_KEY_ID"] = AlpacaAuth["alpacakey"]
    os.environ["APCA_API_SECRET_KEY"] = AlpacaAuth["alpacasecret"]
