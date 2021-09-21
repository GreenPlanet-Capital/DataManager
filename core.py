import configparser
import os

configParse = configparser.ConfigParser()
configParse.read(os.path.join('config_files', 'assetConfig.cfg'))
AlpacaAuth = {
    'AlpacaKey': configParse.get('Alpaca', 'AlpacaKey'),
    'AlpacaSecret': configParse.get('Alpaca', 'AlpacaSecret'),
}
IEXAuthSandbox = {
    'PublicKey': configParse.get('IEX_Sandbox', 'IEX_Sandbox_Public'),
    'PrivateKey': configParse.get('IEX_Sandbox', 'IEX_Sandbox_Private'),
}
IEXAuth = {
    'PublicKey': configParse.get('IEX_Real', 'IEX_Public'),
    'PrivateKey': configParse.get('IEX_Real', 'IEX_Private'),
}
SANDBOX = configParse.get('Globals', 'UseSandbox')


def setEnv():
    if SANDBOX:
        os.environ["IEX_TOKEN"] = IEXAuthSandbox['PrivateKey']
        os.environ["IEX_API_VERSION"] = "iexcloud-sandbox"
    else:
        os.environ["IEX_TOKEN"] = IEXAuth['PrivateKey']
    os.environ["APCA_API_BASE_URL"] = "https://paper-api.alpaca.markets/"
    os.environ["APCA_API_KEY_ID"] = AlpacaAuth['AlpacaKey']
    os.environ["APCA_API_SECRET_KEY"] = AlpacaAuth['AlpacaSecret']
