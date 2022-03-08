"""
2018-2022 -> not all stocks have the same trading days eventhough
they're part of the same exchange

using mcal, get all trading days between two dates
after we get data, ensure
"""

from datetime import datetime
from DataManager.datamgr import data_manager
from DataManager.utils.timehandler import TimeHandler

start_timestamp_dt = datetime(2020, 1, 1)
end_timestamp_dt = datetime(2022, 2, 22)
exchangeName = "NYSE"
limit = 15
update_before = False

this_manager = data_manager.DataManager(
    limit=limit,
    update_before=update_before,
    exchangeName=exchangeName,
    isDelisted=False,
)

start_timestamp: str = TimeHandler.get_string_from_datetime(start_timestamp_dt)
end_timestamp: str = TimeHandler.get_string_from_datetime(end_timestamp_dt)

dict_of_dfs = this_manager.get_stock_data(start_timestamp, end_timestamp, api="Alpaca")

list_of_final_symbols = this_manager.list_of_symbols
