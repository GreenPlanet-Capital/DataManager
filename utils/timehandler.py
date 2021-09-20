from datetime import datetime
import numpy as np


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
