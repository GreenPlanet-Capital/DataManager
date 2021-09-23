from datetime import datetime
from time import strftime
import numpy as np
from pandas import Timestamp


class TimeHandler:
    @staticmethod
    def get_string_from_datetime(inputString: datetime) -> str:
        return datetime.strftime(inputString, '%Y-%m-%d %H:%M:%S')

    @staticmethod
    def get_datetime_from_string(inputDatetime: str) -> datetime:
        return datetime.strptime(inputDatetime, '%Y-%m-%d %H:%M:%S')

    @staticmethod
    def get_string_from_datetime64(inputDatetime64: np.datetime64) -> str:
        return datetime.utcfromtimestamp((inputDatetime64 - np.datetime64('1970-01-01T00:00:00Z'))
                                         / np.timedelta64(1, 's')).strftime('%Y-%m-%d %H:%M:%S')

    @staticmethod
    def get_datetime64_from_string(inputString: str) -> np.datetime64:
        return np.datetime64(inputString)

    @staticmethod
    def get_alpaca_string_from_string(inputString: str) -> str:
        return (datetime.strptime(inputString, '%Y-%m-%d %H:%M:%S')).strftime('%Y-%m-%d')

    @staticmethod
    def get_alpaca_string_from_datetime(datetimeInput: datetime):
        return datetimeInput.strftime('%Y-%m-%d')

    @staticmethod
    # TODO change method name to get_alpaca_string_from_timestamp
    def get_alpaca_string_from_timestamp(timestampInput: Timestamp):
        return TimeHandler.get_alpaca_string_from_datetime(timestampInput.date())

    @staticmethod
    def get_string_from_timestamp(timestampInput: Timestamp):
        return TimeHandler.get_string_from_datetime(timestampInput.date())
