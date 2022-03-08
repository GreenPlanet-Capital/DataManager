from datetime import datetime, timezone
import os
from pandas import Index, Timestamp
from typing import Any, Dict, List, Tuple
import warnings
import numpy as np
import pandas as pd
from pandas.core.frame import DataFrame
import pandas_market_calendars as mcal
import math
import concurrent.futures
import timeit
from DataManager.utils.timehandler import TimeHandler
from DataManager.utils.conversions import _Conversions
from DataManager.database_layer.tables import DailyDataTableManager, MainTableManager
from DataManager.core import DATAMGR_ABS_PATH
from DataManager.assetmgr.asset_manager import Assets
from DataManager.database_layer.database import DatabaseManager
from DataManager.datamgr.data_extractor import DataExtractor

api_start = 0.0
api_end = 0.0


class DataManager:
    """
    Provides financial data for a certain set of stock symbols.

    Example:
    data = DataManager(limit=10, update_before=True, exchangeName = 'NYSE', isDelisted=True)
    list_of_final_symbols = data.list_of_symbols

    Inputs:
        - Keyword args **criteria:
            - `None` or `{}` to get all symbols
            - `exchangeName` (defaults to 'NYSE')
            - `isDelisted` (defaults to False)
            - `isSuspended` (defaults to False)
            - `index_name` (optional) (N/A)
        - `limit`: sets a limit on the number of symbols used
        - `asset_db_name`: fully qualified path to the AssetDB
        - `stock_db_name`: fully qualified path to the Stock_DataDB
        - `update_before`: if True, updates AssetsDB upon instantiation (defaults to False)
    """

    def __init__(
        self,
        limit=None,
        asset_db_name="AssetDB.db",
        stock_db_name="Stock_DataDB.db",
        update_before=False,
        freq_data="1Day",
        **criteria,
    ):
        self._assets = Assets(asset_db_name)
        self._main_stocks = MainStocks(stock_db_name, self._assets)
        self._extractor = DataExtractor()
        self._daily_stocks = DailyStockTables(self._main_stocks)

        if update_before:
            self._assets.update_all_dbs()
            self._main_stocks.repopulate_all_assets()

        if "isDelisted" not in criteria:
            criteria["isDelisted"] = False

        if "isSuspended" not in criteria:
            criteria["isSuspended"] = False

        if "exchangeName" not in criteria:
            self._basket_of_symbols = (
                self._assets.asset_table_manager.get_symbols_from_criteria(criteria)
            )
            self._exchange_name = "NYSE"
        else:
            self._exchange_name = criteria["exchangeName"]
            self._basket_of_symbols = (
                self._assets.asset_table_manager.get_symbols_from_criteria(criteria)
            )

        if limit:
            if len(self._basket_of_symbols) > limit:
                self._basket_of_symbols = self._basket_of_symbols[:limit]
            else:
                warnings.warn(
                    "Limit is greater than available symbols for defined criteria"
                )

        self.freq_data = freq_data
        self._required_symbols_data, self._required_dates = [], []
        self.list_of_symbols = []

    def reset_required_vars(self):
        self._required_symbols_data, self._required_dates = [], []

    def validate_timestamps(self, start_timestamp, end_timestamp):
        if TimeHandler.get_datetime_from_string(
            start_timestamp
        ) > TimeHandler.get_datetime_from_string(end_timestamp):
            raise ValueError(
                "DateOutOfRange: start timestamp cannot be later than end timestamp"
            )

        thisExchange = mcal.get_calendar(self._exchange_name)
        valid_dates_for_ex = thisExchange.valid_days(
            TimeHandler.get_alpaca_string_from_string(start_timestamp),
            TimeHandler.get_alpaca_string_from_string(end_timestamp),
        )
        new_start, new_end = TimeHandler.get_string_from_timestamp(
            valid_dates_for_ex[0]
        ), TimeHandler.get_string_from_timestamp(valid_dates_for_ex[-1])
        if new_start != start_timestamp:
            print(
                f"NOT A TRADING DAY: Start timestamp has changed from: {start_timestamp} to {new_start}"
            )
        if new_end != end_timestamp:
            print(
                f"NOT A TRADING DAY: End timestamp has changed from: {end_timestamp} to {new_end}"
            )

        return new_start, new_end, valid_dates_for_ex

    def get_stock_data(
        self,
        start_timestamp,
        end_timestamp,
        api="Alpaca",
        fill_data: int = 3,
        threading=True,
    ):

        print("Validating Dates...")
        start_timestamp, end_timestamp, _ = self.validate_timestamps(
            start_timestamp, end_timestamp
        )
        print("Finished validating date\n")

        print("Checking dates availability...")
        for stock in self._basket_of_symbols:
            self.get_one_stock_data(stock, start_timestamp, end_timestamp)
        print("Finished checking dates availability!\n")

        # No data needs to be fetched
        if len(self._required_dates) == 0:
            print("All data is available locally")
            self.list_of_symbols = self._basket_of_symbols
            return self._daily_stocks.get_daily_stock_data(
                self.list_of_symbols, start_timestamp, end_timestamp
            )

        print("Getting data from API.")
        global api_start
        global api_end
        api_start = timeit.default_timer()

        type_data = self.freq_data

        list_tuples, partial_list_symbols = getattr(
            self._extractor, f"getMultipleListHistorical{api}"
        )(
            self._required_symbols_data,
            self._required_dates,
            type_data,
            self._exchange_name,
        )

        list_tuples, ext_partial_symbols = self.fill_list_tuples(
            list_tuples, fill_data, self._required_dates
        )
        partial_list_symbols.extend(ext_partial_symbols)
        api_end = timeit.default_timer()
        print("Finished getting data from API!\n")

        if not (len(list_tuples) == 0) or list_tuples:
            self._daily_stocks.update_daily_stock_data(list_tuples, threading=threading)
        self.reset_required_vars()
        self._extractor.AsyncObj.reset_async_list()

        self.list_of_symbols = list(
            set(self._basket_of_symbols).difference(set(partial_list_symbols))
        )
        return self._daily_stocks.get_daily_stock_data(
            self.list_of_symbols, start_timestamp, end_timestamp
        )

    def fill_list_tuples(self, list_tuples, fill_val, tuples_of_req_dates):
        final_list_tuples = []
        partial_symbols = []
        thisExchange = mcal.get_calendar(self._exchange_name)
        needed_timeframes = set(tuples_of_req_dates)
        timeframe_to_valid_dates: Dict[str, List[Timestamp]] = dict()
        for timeframe in needed_timeframes:
            valid_dates_for_ex = thisExchange.valid_days(timeframe[0], timeframe[1])
            timeframe_to_valid_dates[timeframe] = valid_dates_for_ex

        for i, (tick, df) in enumerate(list_tuples):
            valid_dates_for_ex = timeframe_to_valid_dates[tuples_of_req_dates[i]]
            n_valid_dates = len(valid_dates_for_ex)
            min_len_req = n_valid_dates - fill_val
            len_df = len(df)
            if len_df < min_len_req:  # reject this tuple
                partial_symbols.append(tick)
            elif len_df < n_valid_dates:  # to fix the df
                this_df_dates = set(
                    [TimeHandler.get_string_from_timestamp(date) for date in df.index]
                )
                valid_dates_for_ex = set(
                    [
                        TimeHandler.get_string_from_timestamp(date)
                        for date in valid_dates_for_ex
                    ]
                )
                missing_dates = valid_dates_for_ex.difference(this_df_dates)
                missing_dates = list(missing_dates)
                missing_dates.sort()
                self.fill_missing_dates(df, missing_dates)
                final_list_tuples.append((tick, df))
            else:
                final_list_tuples.append((tick, df))

        return final_list_tuples, partial_symbols

    def fill_missing_dates(self, df: pd.DataFrame, missing_dates: List[str]):
        df["timestamp_strings"] = df.index
        df["timestamp_strings"] = df["timestamp_strings"].apply(
            TimeHandler.get_string_from_timestamp
        )
        timestamp_series = df["timestamp_strings"]
        fallback_timestamp_strings = []
        for missing_date in missing_dates:
            fallback_vals = timestamp_series[timestamp_series < missing_date]
            assert (
                len(fallback_vals) > 0
            ), f"LENGTH ERROR: {missing_date=} does not have a fallback value in the dataframe"
            if fallback_vals.empty:
                fallback_timestamp_strings.append("")
            else:
                fallback_timestamp_strings.append(fallback_vals[-1])

        for to_insert_timestamp_str, fallback_timestamp_str in zip(
            missing_dates, fallback_timestamp_strings
        ):
            if not fallback_timestamp_str:
                continue
            df_temp = df.loc[df["timestamp_strings"] == fallback_timestamp_str].copy()
            t1 = pd.Timestamp(to_insert_timestamp_str).tz_localize("UTC")
            index: Index = [t1]
            df_temp.index = index
            df_temp.index.name = "timestamp"
            df_temp.drop("timestamp_strings", axis=1, inplace=True)
            df = pd.concat([df, df_temp], axis=0)
        df.drop("timestamp_strings", axis=1, inplace=True)
        df.sort_index(inplace=True)

    def get_one_stock_data(self, stock_symbol, start_timestamp, end_timestamp):
        (
            statusTimestamp,
            req_start,
            req_end,
        ) = self._main_stocks.table_manager.check_data_availability(
            stock_symbol, start_timestamp, end_timestamp
        )
        if statusTimestamp:
            if req_start:
                self._required_symbols_data.append(stock_symbol)
                self._required_dates.append(
                    (
                        TimeHandler.get_alpaca_string_from_string(start_timestamp),
                        TimeHandler.get_alpaca_string_from_string(
                            TimeHandler.get_string_from_datetime(req_start)
                        ),
                    )
                )

            if req_end:
                self._required_symbols_data.append(stock_symbol)
                self._required_dates.append(
                    (
                        TimeHandler.get_alpaca_string_from_string(
                            TimeHandler.get_string_from_datetime(req_end)
                        ),
                        TimeHandler.get_alpaca_string_from_string(end_timestamp),
                    )
                )
        else:
            self._required_symbols_data.append(stock_symbol)
            self._required_dates.append(
                (
                    TimeHandler.get_alpaca_string_from_string(start_timestamp),
                    TimeHandler.get_alpaca_string_from_string(end_timestamp),
                )
            )


class MainStocks:
    def __init__(self, db_name, assets: Assets):
        self.db_path = os.path.join(DATAMGR_ABS_PATH, os.path.join("tempDir", db_name))
        self.table_manager = MainTableManager(self.db_path)
        self.assets = assets

    def repopulate_all_assets(self):
        print("Updating StockData Main Database...")

        symbols_list = self.assets.asset_table_manager.get_all_tradable_symbols()

        for symbol in symbols_list:
            self.update_stock_symbol_main_table(symbol)

        print("Update completed\n")

    def update_stock_symbol_main_table(
        self, stockSymbol, dataAvailableFrom=None, dataAvailableTo=None
    ):
        asset_data = {
            "stockSymbol": stockSymbol,
            "dataAvailableFrom": dataAvailableFrom,
            "dataAvailableTo": dataAvailableTo,
            "dateLastUpdated": TimeHandler.get_string_from_datetime(
                datetime.now(timezone.utc)
            ),
        }
        main_asset_data = self.table_manager.get_one_asset(stockSymbol)
        if main_asset_data:
            asset_data["dataAvailableFrom"] = (
                dataAvailableFrom
                if dataAvailableFrom is not None
                else main_asset_data["dataAvailableFrom"]
            )
            asset_data["dataAvailableTo"] = (
                dataAvailableTo
                if dataAvailableTo is not None
                else main_asset_data["dataAvailableTo"]
            )
            self.table_manager.update_asset(asset_data)
        else:
            self.table_manager.insert_asset(asset_data)


class DailyStockTables:
    def __init__(self, main_stocks: MainStocks):
        self.main_stocks = main_stocks
        self.db = self.main_stocks.table_manager.db

    def update_daily_stock_data(
        self,
        list_of_tuples: List[Tuple[str, pd.DataFrame]],
        slice_val=50,
        threading=True,
    ):
        """
        Input: list_of_tuples
        Format: [('SYMBOL1', pandas.Dataframe), ('SYMBOL2', pandas.Dataframe)...]
        """

        print("Updating DailyStockTables Database...")

        # Slice list_of_tuples into groups
        number_of_tuples = len(list_of_tuples)
        slice_val = min(slice_val, number_of_tuples // 10)
        if slice_val == 0:
            slice_val = number_of_tuples
        step_value = math.ceil(number_of_tuples / slice_val)
        groups_of_tuples = []
        for i in range(0, number_of_tuples, step_value):
            end_value = i + step_value
            if end_value > number_of_tuples:
                end_value = number_of_tuples
            groups_of_tuples.append(list_of_tuples[i:end_value])

        # Create the step_value+1 DBs
        list_main_stock_connections: List[MainStocks] = []
        for i in range(0, len(groups_of_tuples)):
            this_main_stock = MainStocks(
                db_name=os.path.join("threadDir", f"Temp_DB{i}.db"), assets=Assets()
            )
            this_main_stock.table_manager.drop_all_tables(
                exclude=[this_main_stock.table_manager.table_name]
            )
            list_main_stock_connections.append(this_main_stock)

        if threading:
            self.insert_into_dbs_with_threading(
                groups_of_tuples, list_main_stock_connections
            )
        else:
            self.insert_into_dbs_without_threading(
                groups_of_tuples, list_main_stock_connections
            )

        print("Merging temp DBs")
        for main_stocks_connection in list_main_stock_connections:
            list_of_stock_tables = main_stocks_connection.table_manager.list_tables()
            for stock_table_name in list_of_stock_tables:
                if stock_table_name != self.main_stocks.table_manager.table_name:
                    DailyStockDataTable(
                        stock_table_name, self.main_stocks.table_manager.db
                    )
                    # Copying all tables excluding temp DB's MainStockData table
                    main_stocks_connection.table_manager.db.insert_table_into_another_db(
                        self.main_stocks.db_path, stock_table_name
                    )

            main_stocks_connection.table_manager.db.insert_main_table_into_another_db(
                self.main_stocks.db_path, self.main_stocks.table_manager.table_name
            )

        print("Update completed\n")

    def insert_into_dbs_without_threading(
        self,
        groups_of_tuples: List[List[Tuple[str, pd.DataFrame]]],
        list_main_stock_connections,
    ):
        for list_of_tuples, main_stocks_connection in zip(
            groups_of_tuples, list_main_stock_connections
        ):
            self.insert_into_dbs_one_connection(list_of_tuples, main_stocks_connection)
            # print(f"Status: {main_stocks_connection.db_path} is Complete")

    def insert_into_dbs_with_threading(
        self,
        groups_of_tuples: List[List[Tuple[str, pd.DataFrame]]],
        list_main_stock_connections,
    ):

        with concurrent.futures.ThreadPoolExecutor() as executor:
            _: Dict[concurrent.futures.Future[Any], MainStocks] = {
                executor.submit(
                    self.insert_into_dbs_one_connection,
                    list_of_tuples,
                    main_stocks_connection,
                ): main_stocks_connection
                for list_of_tuples, main_stocks_connection in zip(
                    groups_of_tuples, list_main_stock_connections
                )
            }  # This is a dictionary of futures

            # for fut in concurrent.futures.as_completed(futures):
            #     original_task = futures[fut]
            #     # print(f"Status: {original_task.db_path} is {fut.result()}")

        print()

    def insert_into_dbs_one_connection(self, list_of_tuples, main_stocks_connection):
        for stock_symbol, df in list_of_tuples:
            dict_columns_type = {
                "open": float,
                "high": float,
                "low": float,
                "close": float,
                "volume": int,
                "trade_count": int,
                "vwap": float,
            }
            df = df.astype(dict_columns_type)
            self.update_one_stock_table(stock_symbol, df, main_stocks_connection)
        return "Complete"

    def update_one_stock_table(
        self, stock_symbol, df: DataFrame, main_stocks_connection: MainStocks
    ):
        daily_stock_table = DailyStockDataTable(
            stock_symbol, main_stocks_connection.table_manager.db
        )
        records = _Conversions().tuples_to_dict(
            list(df.to_records()), daily_stock_table.table_manager.columns
        )
        dataAvailableFrom, dataAvailableTo = daily_stock_table.update_daily_stock_data(
            records
        )
        main_stocks_connection.update_stock_symbol_main_table(
            stockSymbol=stock_symbol,
            dataAvailableFrom=dataAvailableFrom,
            dataAvailableTo=dataAvailableTo,
        )

    def get_daily_stock_data(
        self, this_list_of_symbols, start_timestamp, end_timestamp
    ):
        dictStockData = {}
        print("Reading data from database.")
        for individualSymbol in this_list_of_symbols:
            thisStockTable = DailyStockDataTable(
                individualSymbol, self.db
            ).table_manager
            listData = thisStockTable.get_data(start_timestamp, end_timestamp)
            thisDf = pd.DataFrame(listData, columns=list(thisStockTable.columns.keys()))
            dictStockData[individualSymbol] = thisDf
        print(
            f"Read complete! Returning dataframe(s) for {len(this_list_of_symbols)} symbols.\n"
        )
        return dictStockData


class DailyStockDataTable:
    def __init__(self, table_name, db: DatabaseManager):
        self.table_manager = DailyDataTableManager(table_name=table_name, db=db)

    def update_daily_stock_data(self, list_of_timestamped_data: List[Dict[str, Any]]):
        """
        Accepts a list of dictionaries of timestamped OHLCVTV data
        Returns: list with the new date available from and date available to
        """
        for timestamp_ohlc_dict in list_of_timestamped_data:
            if isinstance(timestamp_ohlc_dict["timestamp"], np.datetime64):
                timestamp_ohlc_dict[
                    "timestamp"
                ] = TimeHandler.get_string_from_datetime64(
                    timestamp_ohlc_dict["timestamp"]
                )
            if not self.table_manager.get_one_day_data(
                timestamp_ohlc_dict["timestamp"]
            ):
                timestamp_ohlc_dict["volume"] = int(timestamp_ohlc_dict["volume"])
                timestamp_ohlc_dict["trade_count"] = int(
                    timestamp_ohlc_dict["trade_count"]
                )
                self.table_manager.insert_data(timestamp_ohlc_dict)
            else:
                warnings.warn(
                    f'Trying to insert OHLCVTC data that already exists for timestamp: {timestamp_ohlc_dict["timestamp"]}'
                )

        return self.table_manager.get_dates_for_available_data()


# Testing function
def time_it_func(threading):
    assets = Assets("AssetDB.db")
    # assets.update_db_alpaca_assets()
    main_stocks = MainStocks("Stock_DataDB.db", assets)
    main_stocks.table_manager.drop_all_tables()
    main_stocks.table_manager.create_asset_table(
        main_stocks.table_manager.table_name, main_stocks.table_manager.columns
    )
    main_stocks.repopulate_all_assets()

    start = timeit.default_timer()
    data = DataManager(update_before=False, limit=500)
    _ = data.get_stock_data(
        TimeHandler.get_string_from_datetime(datetime(2018, 1, 1)),
        TimeHandler.get_string_from_datetime(datetime(2018, 2, 1)),
        threading=threading,
    )
    _ = data.list_of_symbols
    end = timeit.default_timer()
    return (end - start) - (api_end - api_start)


if __name__ == "__main__":

    tries = 1
    ttime = 0
    msg = ""
    msg += f"Number of repeats: {tries}\n"
    msg += "Time with threading:"

    for trial in range(tries):
        print(f"Trial: {trial + 1}")
        ttime += time_it_func(threading=True)

    msg += f"{ttime / tries}\n"

    msg += "Time without threading:"

    for trial in range(tries):
        print(f"Trial: {trial+1}")
        ttime += time_it_func(False)

    msg += f"{ttime/tries}\n"

    print(msg)

    # dbAddr = f'{os.path.join("tempDir", "Stock_DataDB_Test.db")}'
    # db_manager = DatabaseManager(dbAddr)
    # test_symbol_stock_table = DailyStockDataTable('TEST_SYMBOL_TWO', db_manager)
    # main_stocks = MainStocks('Stock_DataDB_Test.db')

    # listTimestampedData = ({'timestamp': '2021-01-01',
    #                         'open': 150.5655,
    #                         'high': 155.5655,
    #                         'low': 145.5909,
    #                         'close': 148.5390,
    #                         'volume': 2003940,
    #                         'trade_count': 45000,
    #                         'vwap': None,
    #                         },
    #                        {'timestamp': '2021-01-02',
    #                         'open': 152.5655,
    #                         'high': 157.5655,
    #                         'low': 147.5909,
    #                         'close': 150.5390,
    #                         'volume': 345_000,
    #                         'trade_count': 50_000,
    #                         'vwap': None, },
    #                        {'timestamp': '2021-01-03',
    #                         'open': 154.5655,
    #                         'high': 159.5655,
    #                         'low': 149.5909,
    #                         'close': 151.5390,
    #                         'volume': 100_000,
    #                         'trade_count': 35_000,
    #                         'vwap': None, },
    #                        )
    # dFrom, dTo = test_symbol_stock_table.update_daily_stock_data(
    #     list_of_timestamped_data=listTimestampedData)
    # main_stocks.update_stock_symbol_main_table(test_symbol_stock_table.table_manager.table_name,
    #                                            dataAvailableFrom=dFrom, dataAvailableTo=dTo)

    # allStockTables = DailyStockTables(dbAddr, main_stocks)
    # allStockTables.get_daily_stock_data(['TEST_SYMBOL_TWO'], '2021-01-01', '2021-01-03')
