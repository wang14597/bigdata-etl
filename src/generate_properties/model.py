import csv
from typing import Iterable


class PropertyInfo:

    def __dir__(self) -> Iterable[str]:
        super_dir = super.__dir__(self)
        return (i for i in super_dir if not i.__contains__('__'))

    def __str__(self) -> str:
        dir__ = self.__dir__()
        value_param = [getattr(self, i) for i in dir__]
        return ','.join(value_param)


class MigrateDataPropertyInfo(PropertyInfo):

    def __init__(self, *args):
        self.db_type = self.__get_args(args, 0)
        self.db_name = self.__get_args(args, 1)
        self.table_name = self.__get_args(args, 2)
        self.datetime_field = self.__get_args(args, 3)
        self.start = self.__get_args(args, 4)
        self.end = self.__get_args(args, 5)
        self.target_db = self.__get_args(args, 6)
        self.target_table = self.__get_args(args, 7)
        self.task_num = self.__get_args(args, 8)
        self.split_by = self.__get_args(args, 9)

    def __get_args(self, args, i):
        return args[i] if i < len(args) else ''


class CheckDataPropertyInfo(PropertyInfo):

    def __init__(self, *args):
        self.db_name = args[0]
        self.table_name = args[1]
        self.datetime_field = args[2]
        self.start = args[3]
        self.end = args[4]
        self.target_db = args[5]
        self.target_table = args[6]
        self.duration = args[7]
        self.numeric_precision = args[8]
        self.exclusive_columns = args[9]


class ToOdsPropertyInfo(PropertyInfo):

    def __init__(self, *args):
        self.db_name = args[0]
        self.table_name = args[1]
        self.partition_datetime = args[2]
        self.target_db = args[3]
        self.target_table = args[4]
        self.partition = args[5]
        self.columns_relationship = args[6]


class PropertyObjects:

    def __init__(self, csv_path, class_obj):
        self.csv_path = csv_path
        self.class_obj = class_obj

    def get_csv_lines(self):
        lines = open(self.csv_path)
        reader = csv.reader(lines, quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True)
        return list(reader)[1:]

    def generate_obj(self):
        lines = self.get_csv_lines()
        objs = [self.class_obj(*i) for i in lines]
        return objs
