# Data Manager
## Setup
### Create a assetConfig.cfg
- Create an assetConfig.cfg file in the directory called config_files
- Insert your API Keys and Secrets here as follows:
~~~
[Globals]
UseSandbox=False

[Alpaca]
alpacakey=KEY_HERE
alpacasecret=SECRET_HERE

[IEX_Sandbox]
IEX_Sandbox_Public=PUBLIC_HERE
IEX_Sandbox_Private=PRIVATE_HERE

[IEX_Real]
IEX_Public=PUBLIC_HERE
IEX_Private=PRIVATE_HERE
~~~

### Calling from External Directory
```python
import os, sys
sys.path.append('DataManager') # Insert DataManager to path

# Set env variable to absolute path of datamanager folder
os.environ['DATAMGR_ABS_PATH'] = '/home/lifewhiz/projects/DataManager'

# Now import DataManager
from DataManager.datamgr import data_manager
this_manager = data_manager.DataManager(limit=10, update_before=True, exchangeName = 'NYSE', indexName='snp', isDelisted=False)
dict_of_dfs = this_manager.get_stock_data('2018-06-01 00:00:00', 
                                          '2019-06-01 00:00:00',
                                          api='Alpaca')
list_of_final_symbols = this_manager.list_of_symbols
```


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
