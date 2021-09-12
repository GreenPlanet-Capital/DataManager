# Data Manager
## Setup
### Create a assetConfig.cfg
- Create an assetConfig.cfg file in the directory called config_files
- Insert your API Keys and Secrets here as follows:
~~~
[Alpaca]
AlpacaKey=KeyHere
AlpacaSecret=SecretHere

[IEX_Sandbox]
IEX_Sandbox_Public=PublicHere
IEX_Sandbox_Private=PrivateHere

[IEX_Real]
IEX_Public=PublicHere
IEX_Private=PrivateHere
~~~

## Examples of API Returns

### Example of Alpaca API Return 

```json
{ "id": "5d138c1a-7894-4559-9ca7-9ac565dffcac", 
  "class": "us_equity",
  "exchange": "NASDAQ", 
  "symbol": "ELON", 
  "name": "", 
  "status": "inactive", 
  "tradable": "False", 
  "marginable": "False", 
  "shortable": "False", 
  "easy_to_borrow": "False", 
  "fractionable": "False"
}
```

### Example of PyNse API Return

```json
{ "symbol": "JBCHEPHARM", 
  "companyName": "JB Chemicals & Pharmaceuticals Limited",
  "industry": "PHARMACEUTICALS",
  "activeSeries": ["EQ"],
  "debtSeries": [],
  "tempSuspendedSeries": [],
  "isFNOSec": "False",
  "isCASec": "False",
  "isSLBSec": "True",
  "isDebtSec": "False",
  "isSuspended": "False",
  "isETFSec": "False",
  "isDelisted": "False",
  "isin": "INE572A01028"
}
```
