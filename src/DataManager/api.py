from datetime import datetime
import logging
import sys
from typing import Dict, List, Tuple

import pandas as pd
from DataManager.assetmgr.asset_manager import Assets
from DataManager.database_layer.tables import DailyStockTableManager
from DataManager.datamgr.data_extractor import DataExtractor
import DataManager.datamgr.data_manager as data_manager

logging.disable(logging.DEBUG)
logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger()


class QuantifyData:
    def __init__(
        self,
        asset_db_name="AssetDB.db",
        update_asset_database=False,
    ):
        self._assets = Assets(asset_db_name)
        self._datamgr: data_manager.DataManager = data_manager.DataManager()
        self._extractor = DataExtractor()
        self._basket_of_symbols = None
        self._exchange_name = None

        self._required_symbols_data, self._required_dates = [], dict()
        self.list_of_symbols = []

        if update_asset_database:
            self._assets.update_all_dbs()

    def get_data(
        self,
        start_timestamp: datetime,
        end_timestamp: datetime,
        list_of_tickers=None,
        limit=None,
        timeframe="1Day",
        isDelisted=False,
        isSuspended=False,
        exchangeName: str = None,
        download_data: bool = False,
        fill_data: int = 3,
        api: str = "Alpaca",
    ) -> Tuple[List[str], Dict[str, pd.DataFrame]]:
        self._daily_stocks = DailyStockTableManager(timeframe=timeframe)
        criteria: Dict[str:bool] = {
            "isDelisted": isDelisted,
            "isSuspended": isSuspended,
        }
        if exchangeName:
            assert exchangeName in [
                "NYSE",
                "NASDAQ",
            ], "exchangeName can be one of [NYSE, NASDAQ]"
            criteria["exchangeName"] = exchangeName
            self._exchange_name = exchangeName

        if list_of_tickers:
            logging.info("Setting provided list of tickers")
            self._basket_of_symbols = list_of_tickers
        else:
            logging.info("Getting symbols from criteria")
            self._basket_of_symbols = (
                self._assets.asset_table_manager.get_symbols_from_criteria(criteria)
            )
            logging.info(f"Received {len(self._basket_of_symbols)} symbols from Asset Manager")

        if limit:
            if len(self._basket_of_symbols) > limit:
                self._basket_of_symbols = self._basket_of_symbols[:limit]
            else:
                logging.warn(
                    "Limit is greater than available symbols for defined criteria"
                )
                logging.warn("Using maximum available symbols")

        logger.info("Validating timestamps")
        start_timestamp, end_timestamp, _ = data_manager.validate_timestamps(
            start_timestamp, end_timestamp
        )
        logger.info("Finished validating timestamps")

        logger.info("Checking availability of data")
        for stock in self._basket_of_symbols:
            self._datamgr.calculate_dates_to_extract(
                stock, start_timestamp, end_timestamp
            )
        if len(self._datamgr._required_symbols_data) > 0:
            logger.info(
                f"Need data for {len(self._datamgr._required_symbols_data)} symbols"
            )
        logger.info("Finished checking data availability!")

        if len(self._datamgr._required_dates) == 0 or not download_data:
            logger.info("All data is available locally or download_data set to False")
            self.list_of_symbols = list(
                set(self._basket_of_symbols) - set(self._required_symbols_data)
            )
            assert len(self.list_of_symbols) > 0
            return self._daily_stocks.get_daily_stock_data(
                self.list_of_symbols, start_timestamp, end_timestamp
            )

        logger.info("Getting data from API.")

        type_data = timeframe

        list_tuples, partial_list_symbols = getattr(
            self._datamgr._extractor, f"getMultipleListHistorical{api}"
        )(
            self._datamgr._required_symbols_data,
            list(self._datamgr._required_dates.values()),
            type_data,
            self._exchange_name,
        )

        final_list_tuples, ext_partial_symbols = self._datamgr.fill_list_tuples(
            list_tuples, fill_data, self._datamgr._required_dates
        )
        partial_list_symbols.extend(ext_partial_symbols)
        logger.info("Finished getting data from API!\n")

        if len(final_list_tuples) != 0:
            self._daily_stocks.update_daily_stock_data(final_list_tuples)
        else:
            logger.warn("All extracted data was found to be partial.")

        self._datamgr.reset_required_vars()
        self._datamgr._extractor.AsyncObj.reset_async_list()

        self.list_of_symbols = list(
            set(self._basket_of_symbols).difference(set(partial_list_symbols))
        )
        return self.list_of_symbols, self._daily_stocks.get_daily_stock_data(
            self.list_of_symbols, start_timestamp, end_timestamp
        )


if __name__ == "__main__":
    datamgr = QuantifyData()
    symbols, dict_dfs = datamgr.get_data(
        start_timestamp = datetime(2022, 7, 21),
        end_timestamp = datetime(2022, 7, 21),
        exchangeName = "NASDAQ",
        limit = None,
        list_of_tickers=['GOOG'],
        download_data = True
    )
