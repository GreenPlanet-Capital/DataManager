# Data Manager
## Setup
### Create a virtual environment (highly recommended)
~~~shell
foo@bar:~$ python3 -m venv env
foo@bar:~$ source env/bin/activate
~~~
### Install DataManager
- If you are using it as an app
~~~shell
foo@bar:~$ pip3 install git+https://github.com/GreenPlanet-Capital/DataManager@install_b
~~~
- If you are developing DataManager
~~~shell
foo@bar:~$ git clone https://github.com/GreenPlanet-Capital/DataManager@install_b
foo@bar:~$ cd DataManager/
foo@bar:~$ pip install -e .
~~~

### Setup your API Keys
~~~shell
foo@bar:~$ datamgr set api-keys Alpaca AlpacaKey <public-key-here> AlpacaSecret <private-key-here>
~~~

### Local marketstore setup
~~~shell
foo@bar:~$ git clone git@github.com:alpacahq/marketstore.git
foo@bar:~$ cd marketstore
foo@bar:~$ go get -u github.com/alpacahq/marketstore
foo@bar:~$ make install
foo@bar:~$ marketstore init
foo@bar:~$ marketstore start
~~~

### Calling from External Directory
```python
from DataManager.datamgr import data_manager

start_timestamp = '2021-06-01 00:00:00'
end_timestamp = '2021-07-01 00:00:00'
exchangeName = 'NYSE'
limit = 10
update_before = True

this_manager = data_manager.DataManager(
    limit=limit,
    update_before=update_before,
    exchangeName=exchangeName,
    isDelisted=False,
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

### Testing
```shell
foo@bar:~$ python -m coverage run -m pytest
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
