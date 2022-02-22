# Data Manager
## Setup
### Create a virtual environment (highly recommended)
~~~shell
foo@bar:~$ python3 -m venv env
foo@bar:~$ source env/bin/activate
~~~
### Install DataManager
~~~shell
foo@bar:~$ pip3 install git+https://github.com/GreenPlanet-Capital/DataManager@install_b
~~~
### Setup your API Keys
~~~shell
foo@bar:~$ datamgr set api-keys Alpaca AlpacaKey <public-key-here> AlpacaSecret <private-key-here>
~~~

### Calling from External Directory
```python
from DataManager.datamgr import data_manager

start_timestamp = '2021-06-01'
end_timestamp = '2021-07-01'
exchangeName = 'NYSE'
limit = 10
update_before = True

this_manager = data_manager.DataManager(
    limit=limit
    update_before=update_before
    exchangeName=exchangeName,
    isDelisted=False
)

dict_of_dfs = this_manager.get_stock_data(
    start_timestamp,
    end_timestamp,
    api='Alpaca'
)

list_of_final_symbols = this_manager.list_of_symbols
```

### Additional shell commands to datamgr
~~~shell
foo@bar:~$ datamgr show-config
[Alpaca]
alpacakey=KEY HERE
alpacasecret=KEY HERE
~~~
~~~shell
foo@bar:~$ datamgr reset
SUCCESS: Config file was reset
~~~
~~~shell
foo@bar:~$ datamgr uninstall
SUCCESS: Temporary files were deleted.
Deleted <path>/DataManager/config_files/assetConfig.cfg
Deleted <path>/DataManager/tempDir/AssetDB.db
...
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
