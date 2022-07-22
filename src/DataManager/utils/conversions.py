from typing import Dict


class Conversions:
    @staticmethod
    def asset_row_to_dict(columns_dict: Dict[str, str], row):
        return dict(zip(columns_dict.keys(), row))

    @staticmethod
    def tuples_to_dict(list_of_asset_tuples, columns_dict):
        return [
            Conversions.asset_row_to_dict(columns_dict, row)
            for row in list_of_asset_tuples
        ]
