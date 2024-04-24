import json
import os
import sys
from datetime import datetime
from typing import Dict, List
from database import ClickHouse

import pandas as pd

pd.set_option('future.no_silent_downcasting', True)


class Statistics:
    def __init__(self, input_file_path: str, output_folder: str):
        self.input_file_path: str = input_file_path
        self.output_folder: str = output_folder
        self.month_position = {
            'JAN': {},
            'FEB': {},
            'MAR': {},
            'APR': {},
            'MAY': {},
            'JUN': {},
            'JUL': {},
            'AUG': {},
            'SEP': {},
            'OCT': {},
            'NOV': {},
            'DEC': {},
        }
        self.total = []
        self.period = ClickHouse()

    def create_new_key(self, index):
        key = list(self.month_position)[index]
        return ' '.join([key, self.month_position[key]['year']])

    def get_start_end_position(self, index):
        list_month = list(self.month_position)
        try:
            start_index = self.month_position[list_month[index]]['column']
            end_index = self.month_position[list_month[index + 1]]['column']
        except (IndexError, KeyError):
            start_index = self.month_position[list_month[index]]['column']
            end_index = 0
        if start_index > end_index:
            end_index = None
        return start_index, end_index

    def write_to_json(self, direction: str, parsed_data: list) -> None:
        """
        Write data to json.
        """
        basename: str = os.path.basename(f"{self.input_file_path}_{direction}")
        output_file_path: str = os.path.join(self.output_folder, f'{basename}.json')
        with open(f"{output_file_path}", 'w', encoding='utf-8') as f:
            json.dump(parsed_data, f, ensure_ascii=False, indent=4)

    def delete_total_index(self, end_index, df: pd.DataFrame):
        if not end_index:
            df = df.iloc[min(self.total) + 3:]
            self.total.pop(self.total.index(min(self.total)))
            return df
        return df

    @staticmethod
    def get_month_and_year(data_str: dict):
        date_obj = datetime.strptime(list(data_str.keys())[0], '%b %Y')
        month_num = date_obj.month
        year_num = date_obj.year
        return month_num, year_num

    def add_new_columns(self, data_year: List[dict]):
        result = []
        for data_month in data_year:
            month, year = self.get_month_and_year(data_month)
            list_values = list(data_month.values())[0] if list(data_month.values()) else []
            for values in list_values:
                data = {}
                data.update(values)
                data['month'] = month
                data['year'] = year
                data['original_file_name'] = os.path.basename(self.input_file_path)
                data['original_file_parsed_on'] = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                result.append(data)
        return result

    def get_df_month(self, df: pd.DataFrame, start_index, end_index):
        if not end_index:
            df_month = df.iloc[:, start_index:]
        else:
            df_month = df.iloc[:, start_index: end_index]
        df_month = df_month.iloc[:min(self.total) + 1]

        if len(df_month.iloc[0]) > 11:
            df_month = df_month.iloc[:, : 11]

        return df_month

    def get_df_ship_name(self, df: pd.DataFrame):
        df_ship_name = df.iloc[:min(self.total) + 1, 0]
        df_ship_name.iloc[1] = 'shipping_line'
        return df_ship_name

    def parse_data(self, df: pd.DataFrame):
        result_month = []
        df = self.get_month_coordinates(df)
        for index in range(len(self.month_position)):
            if self.check_month(index):
                continue
            start_index, end_index = self.get_start_end_position(index)

            df_month = self.get_df_month(df, start_index, end_index)
            df_ship_name = self.get_df_ship_name(df)
            df = self.delete_total_index(end_index, df)
            df_dict = self.get_information_to_df(df_ship_name, df_month)
            result_month.append({self.create_new_key(index): df_dict})
        return result_month

    @staticmethod
    def get_information_to_df(df_ship_name, df_month):
        result_df = pd.concat([df_ship_name, df_month], axis=1, ignore_index=True).iloc[1:]
        result_df: pd.DataFrame = result_df.dropna(how='all')
        result_df.dropna(axis='columns', how='all', inplace=True)
        result_df = result_df.reset_index(drop=True)
        result_df.columns = result_df.iloc[0]
        result_df = result_df.drop(0)
        result_df.reset_index(drop=True, inplace=True)
        result_df.fillna(0, inplace=True)
        if 'DAL ZAVOD' in result_df.columns:
            result_df.rename(columns={'DAL ZAVOD': 'DAL_ZAVOD'}, inplace=True)
        df_dict = result_df.to_dict(orient='records')
        return df_dict

    def check_month(self, index: int):
        return not self.month_position[list(self.month_position)[index]]

    def get_month_coordinates(self, df: pd.DataFrame):
        for index, row in df.iterrows():
            for column_name, value in row.items():
                for key in self.month_position:
                    if value and isinstance(value, str):
                        if key in value and len(value.split("'")) > 1:
                            self.month_position[key].update(
                                {'row': index, 'column': int(column_name.split(':')[-1].strip()),
                                 'year': value.split("'")[1]})
                        if value in ('TOTAL',) and int(column_name.split(':')[-1].strip()) == 0:
                            if index not in self.total:
                                self.total.append(index)
        df = df.iloc[:max(self.total) + 1]
        return df

    def filter_data_to_period(self, dict_data, sheet_name):
        period = self.period.nw_period if 'NW' in sheet_name else self.period.fea_period
        if not period:
            return dict_data
        period_time = datetime.strptime(period, '%Y-%m')
        filter_dict_data = [list(key)[0] for key in dict_data if
                            datetime.strptime(list(key)[0], '%b %Y') > period_time]
        dict_data = [key for key in dict_data if list(key)[0] in filter_dict_data]
        return dict_data

    def main(self):
        all_sheets = pd.read_excel(self.input_file_path, sheet_name=None, skiprows=1)
        for sheet_name in all_sheets:
            parse_data = self.parse_data(all_sheets[sheet_name])
            filter_data = self.filter_data_to_period(parse_data, sheet_name)
            result = self.add_new_columns(filter_data)
            if not result:
                continue
            self.write_to_json(sheet_name.lower(), result)


if __name__ == "__main__":
    report_order: Statistics = Statistics(sys.argv[1], sys.argv[2])
    report_order.main()
