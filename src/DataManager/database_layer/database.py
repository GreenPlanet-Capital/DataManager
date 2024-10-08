import sqlite3
import numpy as np
from DataManager.utils.timehandler import TimeHandler


class DatabaseManager:
    def __init__(self, database_path):
        self.connection = sqlite3.connect(database_path, check_same_thread=False)

    def __del__(self):
        self.connection.close()

    def _execute(self, statement, values=None, many=False):
        with self.connection:
            cursor = self.connection.cursor()
            if many:
                cursor.executemany(statement, values or [])
            else:
                cursor.execute(statement, values or [])
            return cursor

    def create_table(self, table_name, columns):
        columns_with_types = [
            f"{column_name} {data_type}" for column_name, data_type in columns.items()
        ]
        self._execute(
            f"""
            CREATE TABLE IF NOT EXISTS "{table_name}"
            ({', '.join(columns_with_types)});
            """
        )

    def drop_table(self, table_name):
        self._execute(f'DROP TABLE "{table_name}";')

    def add(self, table_name, data):
        placeholders = ", ".join("?" * len(data))
        column_names = ", ".join(data.keys())
        column_values = tuple(data.values())

        self._execute(
            f"""
            INSERT INTO "{table_name}"
            ({column_names})
            VALUES ({placeholders});
            """,
            column_values,
        )

    def add(self, table_name, data):
        placeholders = ", ".join("?" * len(data))
        column_names = ", ".join(data.keys())
        column_values = tuple(data.values())

        self._execute(
            f"""
            INSERT INTO "{table_name}"
            ({column_names})
            VALUES ({placeholders});
            """,
            column_values,
        )

    def add_many(self, table_name, data):
        placeholders = ", ".join("?" * len(data[0]))
        column_names = ", ".join(data[0].keys())
        values = [tuple(d.values()) for d in data]

        self._execute(
            f"""
            INSERT OR REPLACE INTO "{table_name}"
            ({column_names})
            VALUES ({placeholders});
            """,
            values,
            many=True,
        )

    def delete(self, table_name, criteria):
        placeholders = [f"{column} = ?" for column in criteria.keys()]
        delete_criteria = " AND ".join(placeholders)
        self._execute(
            f"""
            DELETE FROM "{table_name}"
            WHERE {delete_criteria};
            """,
            tuple(criteria.values()),
        )

    def select(self, table_name, criteria=None, order_by=None):
        criteria = criteria or {}

        query = f'SELECT * FROM "{table_name}"'

        if criteria:
            placeholders = [f"{column} = ?" for column in criteria.keys()]
            select_criteria = " AND ".join(placeholders)
            query += f" WHERE {select_criteria}"

        if order_by:
            query += f" ORDER BY {order_by}"

        return self._execute(
            query + ";",
            tuple(criteria.values()),
        )

    def select_between_range(self, table_name, criteria=None, order_by=None):
        criteria = criteria or {}

        query = f'SELECT * FROM "{table_name}"'

        start_timestamp = criteria["start_timestamp"]
        end_timestamp = criteria["end_timestamp"]

        # set the end_timestamp to XX-XX-XX 23:59:59 to avoid off by one
        end_timestamp_datetime = TimeHandler.get_datetime_from_string(end_timestamp)
        end_timestamp_datetime = end_timestamp_datetime.replace(
            hour=23, minute=59, second=59
        )
        end_timestamp = TimeHandler.get_string_from_datetime(end_timestamp_datetime)
        values = start_timestamp, end_timestamp

        if criteria:
            placeholders = ["timestamp >= ?", "timestamp <= ?"]
            select_criteria = " AND ".join(placeholders)
            query += f" WHERE {select_criteria}"

        if order_by:
            query += f" ORDER BY {order_by}"

        return self._execute(
            query + ";",
            values,
        )

    def select_max_value_from_column(self, table_name, column):
        query = f'SELECT MAX({column}) FROM "{table_name}";'
        return self._execute(query)

    def select_min_value_from_column(self, table_name, column):
        query = f'SELECT MIN({column}) FROM "{table_name}";'
        return self._execute(query)

    def select_column_value(self, table_name, stock_symbol, column):
        query = (
            f"SELECT {column} FROM '{table_name}' WHERE stockSymbol='{stock_symbol}';"
        )
        return self._execute(query)

    def list_tables(self):
        query = """SELECT name
                FROM sqlite_master
                WHERE type ='table' AND
                name NOT LIKE 'sqlite_%';
                """
        return self._execute(query)

    def update(self, table_name, criteria, data):
        update_placeholders = [f"{column} = ?" for column in criteria.keys()]
        update_criteria = " AND ".join(update_placeholders)

        data_placeholders = ", ".join(f"{key} = ?" for key in data.keys())

        values = tuple(data.values()) + tuple(criteria.values())

        self._execute(
            f"""
            UPDATE "{table_name}"
            SET {data_placeholders}
            WHERE {update_criteria};
            """,
            values,
        )

    def update_many(self, table_name, criteria, data):
        update_placeholders = [f"{column} = ?" for column in criteria.keys()]
        update_criteria = " AND ".join(update_placeholders)
        values = [tuple(d.values()) + tuple(criteria.values()) for d in data]

        self._execute(
            f"""
            UPDATE "{table_name}"
            SET {update_placeholders}
            WHERE {update_criteria};
            """,
            values,
            many=True,
        )

    def insert_table_into_another_db(self, attach_db_path, table_name):
        query = f"""
                ATTACH DATABASE '{attach_db_path}' AS other;
                """
        self._execute(query)
        query = f"""
                INSERT or REPLACE INTO other."{table_name}"
                SELECT * FROM main."{table_name}";
                """
        self._execute(query)
        query = """
                DETACH other;
                """
        self._execute(query)

    def insert_main_table_into_another_db(self, attach_db_path, main_table_name):
        query = f"""
                ATTACH DATABASE '{attach_db_path}' AS other;
                """
        self._execute(query)

        query = f"SELECT * from main.{main_table_name};"

        for ticker_, start_timestamp, end_timestamp, _ in self._execute(
            query
        ).fetchall():
            query = (
                f'SELECT * from other.{main_table_name} WHERE stockSymbol="{ticker_}";'
            )
            _, second_start_ts, second_end_ts, updated_time = self._execute(
                query
            ).fetchone()
            list_all = [start_timestamp, end_timestamp, second_start_ts, second_end_ts]
            list_all = [x for x in list_all if x is not None]
            final_start, final_end = min(list_all), max(list_all)

            query = f"""
                    INSERT or REPLACE INTO other."{main_table_name}"
                    (stockSymbol ,dataAvailableFrom, dataAvailableTo, dateLastUpdated)
                    VALUES ('{ticker_}', '{final_start}', '{final_end}', '{updated_time}');
                    """
            self._execute(query)

        query = """
                DETACH other;
                """
        self._execute(query)
