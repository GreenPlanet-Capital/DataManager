from pandas import Index, Timestamp
from typing import Any, Dict, List, Tuple
import warnings
import pandas as pd
import pandas_market_calendars as mcal
from DataManager.database_layer.tables import DailyStockTableManager
from DataManager.utils.timehandler import TimeHandler
from DataManager.assetmgr.asset_manager import Assets
from DataManager.datamgr.data_extractor import DataExtractor


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
        update_before=False,
        freq_data="1Day",
        **criteria,
    ):
        self._assets = Assets(asset_db_name)
        self._extractor = DataExtractor()
        self._daily_stocks = DailyStockTableManager(timeframe=freq_data)

        if update_before:
            self._assets.update_all_dbs()

        if "isDelisted" not in criteria:
            criteria["isDelisted"] = False

        if "isSuspended" not in criteria:
            criteria["isSuspended"] = False

        if "exchangeName" not in criteria:
            self._basket_of_symbols = set(
                self._assets.asset_table_manager.get_symbols_from_criteria(criteria)
            )
            self._exchange_name = "NYSE"
        else:
            self._exchange_name = criteria["exchangeName"]
            self._basket_of_symbols = set(
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
        self._required_symbols_data, self._required_dates = [], dict()
        self.list_of_symbols = []

    def reset_required_vars(self):
        self._required_symbols_data, self._required_dates = [], dict()

    def validate_timestamps(
        self, start_timestamp, end_timestamp
    ) -> Tuple[str, str, List[Any]]:
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
        fetch_data: bool = True,
        ensure_full_data: bool = True,
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
        if len(self._required_dates) == 0 or fetch_data is False:
            print("All data is available locally or fetch_data set to False")
            self.list_of_symbols = list(
                set(self._basket_of_symbols) - set(self._required_symbols_data)
            )
            return self._daily_stocks.get_daily_stock_data(
                self.list_of_symbols, start_timestamp, end_timestamp, ensure_full_data
            )

        print("Getting data from API.")

        type_data = self.freq_data

        list_tuples, partial_list_symbols = getattr(
            self._extractor, f"getMultipleListHistorical{api}"
        )(
            self._required_symbols_data,
            list(self._required_dates.values()),
            type_data,
            self._exchange_name,
        )

        final_list_tuples, ext_partial_symbols = self.fill_list_tuples(
            list_tuples, fill_data, self._required_dates
        )
        partial_list_symbols.extend(ext_partial_symbols)
        print("Finished getting data from API!\n")

        if len(final_list_tuples) != 0:
            self._daily_stocks.update_daily_stock_data(final_list_tuples)
        else:
            print("All extracted data was found to be partial.")

        self.reset_required_vars()
        self._extractor.AsyncObj.reset_async_list()

        self.list_of_symbols = list(
            set(self._basket_of_symbols).difference(set(partial_list_symbols))
        )
        return self._daily_stocks.get_daily_stock_data(
            self.list_of_symbols, start_timestamp, end_timestamp, ensure_full_data
        )

    def fill_list_tuples(self, list_tuples, fill_val, dict_of_req_dates):
        final_list_tuples = []
        partial_symbols = []
        thisExchange = mcal.get_calendar(self._exchange_name)
        needed_timeframes = set(dict_of_req_dates.values())
        timeframe_to_valid_dates: Dict[str, List[Timestamp]] = dict()
        for timeframe in needed_timeframes:
            valid_dates_for_ex = thisExchange.valid_days(timeframe[0], timeframe[1])
            timeframe_to_valid_dates[timeframe] = valid_dates_for_ex

        for i, (tick, df) in enumerate(list_tuples):
            valid_dates_for_ex = timeframe_to_valid_dates[dict_of_req_dates[tick]]
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
                df = self.fill_missing_dates(df, missing_dates)
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
        return df

    def get_one_stock_data(self, stock_symbol, start_timestamp, end_timestamp):
        (
            statusTimestamp,
            req_start,
            req_end,
        ) = self._daily_stocks.check_data_availability(
            stock_symbol, start_timestamp, end_timestamp
        )
        if statusTimestamp:

            if req_start and req_end:
                self._required_symbols_data.append(stock_symbol)
                self._required_dates[stock_symbol] = (
                    TimeHandler.get_alpaca_string_from_string(
                        TimeHandler.get_string_from_datetime(req_start)
                    ),
                    TimeHandler.get_alpaca_string_from_string(
                        TimeHandler.get_string_from_datetime(req_end)
                    ),
                )

            elif req_start:
                self._required_symbols_data.append(stock_symbol)
                self._required_dates[stock_symbol] = (
                    TimeHandler.get_alpaca_string_from_string(start_timestamp),
                    TimeHandler.get_alpaca_string_from_string(
                        TimeHandler.get_string_from_datetime(req_start)
                    ),
                )

            elif req_end:
                self._required_symbols_data.append(stock_symbol)
                self._required_dates[stock_symbol] = (
                    TimeHandler.get_alpaca_string_from_string(
                        TimeHandler.get_string_from_datetime(req_end)
                    ),
                    TimeHandler.get_alpaca_string_from_string(end_timestamp),
                )

        else:
            self._required_symbols_data.append(stock_symbol)
            self._required_dates[stock_symbol] = (
                TimeHandler.get_alpaca_string_from_string(start_timestamp),
                TimeHandler.get_alpaca_string_from_string(end_timestamp),
            )


if __name__ == "__main__":
    start_timestamp = "2021-07-06 00:00:00"
    end_timestamp = "2022-07-17 00:00:00"

    exchangeName = "NYSE"
    limit = None
    update_before = False

    this_manager = DataManager(
        limit=limit,
        update_before=update_before,
        exchangeName=exchangeName,
        isDelisted=False,
    )

    dict_of_dfs = this_manager.get_stock_data(
        start_timestamp, end_timestamp, api="Alpaca"
    )

    list_of_final_symbols = this_manager.list_of_symbols
    assert len(set([len(df) for _, df in dict_of_dfs.items()])) == 1
