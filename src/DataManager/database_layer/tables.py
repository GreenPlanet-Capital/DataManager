from datetime import timedelta
from typing import Dict, List, Tuple
import warnings
import numpy as np
import pandas as pd
import pymarketstore as pymkts
from pandas import DataFrame
from collections import Counter

from DataManager.database_layer.database import DatabaseManager
from DataManager.utils.conversions import Conversions
from DataManager.utils.timehandler import TimeHandler


class TableManager:
    def __init__(self, db_name):
        self.db = DatabaseManager(db_name)
        self.table_name = ""
        self.columns = {}

    def __del__(self):
        del self.db

    def list_tables(self):
        return [e for (e,) in self.db.list_tables().fetchall()]

    def create_asset_table(self, table_name, columns):
        self.db.create_table(f"{table_name}", columns)

    def drop_all_tables(self, exclude: List[str] = []):
        tables = self.list_tables()
        for table in tables:
            if table not in exclude:
                self.db.drop_table(table_name=table)

    def insert_assets(self, asset_data):
        self.db.add_many(self.table_name, asset_data)

    def update_asset(self, asset_data):
        self.db.update(
            self.table_name, {"stockSymbol": asset_data["stockSymbol"]}, asset_data
        )

    def get_assets_list(self):
        list_of_assets = self.db.select(self.table_name).fetchall()
        return Conversions.tuples_to_dict(list_of_assets, self.columns)

    def get_one_asset(self, stock_symbol):
        asset = self.db.select(
            self.table_name, {"stockSymbol": stock_symbol}
        ).fetchone()
        return Conversions.asset_row_to_dict(self.columns, asset) if asset else None

    def get_columns(self):
        return self.columns


class AssetTableManager(TableManager):
    def __init__(self, db_name):
        super().__init__(db_name)
        self.table_name = "Assets"
        self.columns = {
            "stockSymbol": "text not null primary key",
            "companyName": "text not null",
            "exchangeName": "text not null",
            "index_name": "text",
            "dateLastUpdated": "text not null",
            "region": "text",
            "currency": "text",
            "isDelisted": "integer",
            "isShortable": "integer",
            "isSuspended": "integer",
        }
        self.create_asset_table(self.table_name, self.columns)

    def get_exchange_basket(self, exchangeName, isDelisted=False, isSuspended=False):
        list_of_assets = self.db.select(
            self.table_name,
            {
                "exchangeName": exchangeName,
                "isDelisted": isDelisted,
                "isSuspended": isSuspended,
            },
        ).fetchall()
        return Conversions.tuples_to_dict(list_of_assets, self.columns)

    def get_index_basket(self, index_name, isDelisted=False, isSuspended=False):
        list_of_assets = self.db.select(
            self.table_name,
            {
                "index_name": index_name,
                "isDelisted": isDelisted,
                "isSuspended": isSuspended,
            },
        ).fetchall()
        return Conversions.tuples_to_dict(list_of_assets, self.columns)

    def get_all_tradable_symbols(self, isDelisted=False, isSuspended=False):
        list_of_assets = self.db.select(
            self.table_name, {"isDelisted": isDelisted, "isSuspended": isSuspended}
        ).fetchall()
        return [
            asset["stockSymbol"]
            for asset in Conversions.tuples_to_dict(list_of_assets, self.columns)
        ]

    def get_symbols_from_criteria(self, criteria):
        list_of_assets = self.db.select(self.table_name, criteria).fetchall()
        return [
            asset["stockSymbol"]
            for asset in Conversions.tuples_to_dict(list_of_assets, self.columns)
        ]


class DailyStockTableManager:
    def __init__(self, timeframe: str):
        self.pym_cli = pymkts.Client(endpoint="http://alpaca-marketstore:5993/rpc")
        self.set_symbols = set(self.pym_cli.list_symbols())
        self.timeframe = timeframe[:2]

    def check_data_availability(self, stock_symbol, start_timestamp, end_timestamp):
        start_timestamp = TimeHandler.get_datetime_from_string(start_timestamp)
        end_timestamp = TimeHandler.get_datetime_from_string(end_timestamp)

        if stock_symbol in self.set_symbols:
            all_dates = list(
                self.pym_cli.sql(
                    [f"SELECT Epoch FROM `{stock_symbol}/{self.timeframe}/OHLCV`;"]
                )
                .first()
                .df()
                .index
            )
        else:
            return False, start_timestamp, end_timestamp

        dataAvailableFrom = all_dates[0].to_pydatetime()
        dataAvailableTo = all_dates[-1].to_pydatetime()

        if (start_timestamp.date() < dataAvailableFrom.date()) and (
            end_timestamp.date() < dataAvailableFrom.date()
        ):  # req_start and req_end are before available dates
            return False, start_timestamp, end_timestamp
        if (start_timestamp.date() > dataAvailableTo.date()) and (
            end_timestamp.date() > dataAvailableTo.date()
        ):  # req_start and req_end are after available dates
            return False, start_timestamp, end_timestamp
        if (start_timestamp.date() < dataAvailableFrom.date()) and (
            end_timestamp.date() <= dataAvailableTo.date()
        ):  # end_date is within range, but start_date is left of available
            return True, dataAvailableFrom - timedelta(days=1), None
        if (start_timestamp.date() >= dataAvailableFrom.date()) and (
            end_timestamp.date() > dataAvailableTo.date()
        ):  # start_date is within range, but end_date is right of available
            return True, None, dataAvailableTo + timedelta(days=1)
        if (start_timestamp.date() < dataAvailableFrom.date()) and (
            end_timestamp.date() > dataAvailableTo.date()
        ):  # start_date is left of available, end_date is right of available
            return (
                True,
                dataAvailableFrom - timedelta(days=1),
                dataAvailableTo + timedelta(days=1),
            )
        else:
            return True, None, None

    def update_daily_stock_data(self, list_of_tuples: List[Tuple[str, pd.DataFrame]]):
        """
        Input: list_of_tuples
        Format: [('SYMBOL1', pandas.Dataframe), ('SYMBOL2', pandas.Dataframe)...]
        """

        print("Updating DailyStockTables Database...")

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
            self.update_one_stock_table(stock_symbol, df)

        print("Update completed!")

    def update_one_stock_table(self, stock_symbol, df: DataFrame):
        df_updated = df.reset_index()
        df_updated["timestamp"] = df_updated["timestamp"].apply(
            lambda d: d.replace(hour=0, minute=0, second=0).value // 10**9
        )
        df_updated = df_updated.rename(columns={"timestamp": "Epoch", "vwap": "vmap"})

        records = df_updated.to_records(index=False)
        data = np.array(records, dtype=records.dtype.descr)

        response = self.pym_cli.write(
            data, f"{stock_symbol}/{self.timeframe}/OHLCV", isvariablelength=True
        )
        assert (
            response is not None and response["responses"] is None
        ), "Error in updating data in database."

    def get_daily_stock_data(
        self,
        this_list_of_symbols,
        start_timestamp,
        end_timestamp,
        ensure_full_data,
        ensure_full_date_strat: str = "mode",
    ):
        dictStockData = {}
        print("Reading data from database.")
        for individual_symbol in this_list_of_symbols:
            dictStockData[individual_symbol] = self.get_specific_stock_data(
                individual_symbol, start_timestamp, end_timestamp
            )
        print(
            f"Read complete! Returning dataframe(s) for {len(this_list_of_symbols)} symbols."
        )

        if ensure_full_data:
            self.full_data_strat(
                dictStockData, this_list_of_symbols, ensure_full_date_strat
            )

        return dictStockData

    def get_specific_stock_data(self, stock_name, start_timestamp, end_timestamp):
        this_params = pymkts.Params(
            stock_name, self.timeframe, "OHLCV", start_timestamp, end_timestamp
        )

        try:
            this_df = self.pym_cli.query(this_params).first().df().reset_index()
        except Exception as e:
            warnings.warn(f"Error encountered: {e}")
            return pd.DataFrame()

        this_df.rename(columns={"Epoch": "timestamp"}, inplace=True)
        this_df["timestamp"] = this_df["timestamp"].apply(
            lambda d: TimeHandler.get_string_from_datetime64(d.tz_convert(None))
        )
        return this_df

    def full_data_strat(
        self, dt_df: Dict[str, pd.DataFrame], list_symbols: List[str], strat: str
    ):
        timeframes = [len(df) for df in dt_df.values()]
        cnts = Counter(timeframes)

        if strat == "max":
            # Remove dataframes with less data than the timeframe
            max_timeframe = max(timeframes)

            for symbol in list(dt_df.keys()):
                df = dt_df[symbol]
                if len(df) < max_timeframe:
                    dt_df.pop(symbol)
                    list_symbols.remove(symbol)  # Hack: fix sometime

            print(
                f"Dataframes with less than {max_timeframe} entries removed. {len(list_symbols)} symbols remaining.\n"
            )
        elif strat == "mode":
            # Remove dataframes with less data than the mode of the timeframes
            mode_timeframe = cnts.most_common(1)[0][0]

            for symbol in list(dt_df.keys()):
                df = dt_df[symbol]
                if len(df) != mode_timeframe:
                    dt_df.pop(symbol)
                    list_symbols.remove(symbol)

            print(
                f"Dataframes with != {mode_timeframe} entries removed. {len(list_symbols)} symbols remaining.\n"
            )
        else:
            raise ValueError(
                "Invalid strategy for ensuring full data. Should be 'max' or 'mode'."
            )
