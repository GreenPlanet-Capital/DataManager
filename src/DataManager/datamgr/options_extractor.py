from datetime import datetime
from typing import Tuple
import mysql.connector
import atexit
import pandas as pd
from pathlib import Path

TICKERS_LOCAL_PATH = Path.home() / ".dolt" / "dbs" / "op_tickers.txt"


class OptionsExtractor:
    def __init__(self):
        print("Connecting to dolt database")
        self.connection = mysql.connector.connect(
            user="root", host="127.0.0.1", port="6000", database="options"
        )
        self.get_or_load_tickers()
        atexit.register(self.cleanup)

    def get_or_load_tickers(self):
        if TICKERS_LOCAL_PATH.exists():
            print("Loading tickers from local file.")
            with open(TICKERS_LOCAL_PATH, "r") as f:
                self.tickers = set(f.read().splitlines())
        else:
            print("Fetching tickers from database. This may take a while.")
            query_res = self.get_query_result(
                "SELECT DISTINCT act_symbol FROM option_chain"
            )
            self.tickers = set(query_row[0] for query_row in query_res)
            with open(TICKERS_LOCAL_PATH, "w") as f:
                f.write("\n".join(self.tickers))

    def construct_query(
        self,
        table: str,
        ticker: str = None,
        date_range: Tuple[datetime, datetime] = None,
        expiration_range: Tuple[datetime, datetime] = None,
        strike: float = None,
        option_type: str = None,
        limit: int = None,
    ):
        q = f"SELECT * FROM {table}"
        conds = []

        if ticker is not None:
            conds.append(f"act_symbol = '{ticker}'")

        if date_range is not None:
            conds.append(f"date BETWEEN '{date_range[0]}' AND '{date_range[1]}'")

        if expiration_range is not None:
            conds.append(
                f" expiration BETWEEN '{expiration_range[0]}' AND '{expiration_range[1]}'"
            )

        if strike is not None:
            conds.append(f"strike = {strike}")

        if option_type is not None:
            assert option_type.lower() in [
                "call",
                "put",
            ], "Invalid option type, must be either 'call' or 'put'"
            conds.append(f"call_put = '{option_type.lower().capitalize()}'")

        if conds:
            q += " WHERE " + " AND ".join(conds)

        if limit is not None:
            q += f" LIMIT {limit}"

        return q

    def get_query_result(self, query):
        cursor = self.connection.cursor()
        cursor.execute(query)
        return cursor.fetchall()

    def get_query_result_pd(self, query):
        return pd.read_sql(query, con=self.connection)

    def cleanup(self):
        print("Closing connection to dolt database")
        self.connection.close()


if __name__ == "__main__":
    ope = OptionsExtractor()
    df = ope.get_query_result_pd(
        ope.construct_query(
            table="option_chain",
            ticker="AAPL",
            date_range=(datetime(2021, 1, 1), datetime(2021, 1, 31)),
            expiration_range=(datetime(2021, 2, 1), datetime(2021, 2, 28)),
            limit=100,
        )
    )
