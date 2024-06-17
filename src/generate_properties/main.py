import sys

from model import *
from generate_tools import *


def main(config_dir):
    for dir_path in get_directories(config_dir):
        csv_path = os.path.join(dir_path, 'csv')
        properties_path = os.path.join(dir_path, 'properties')
        if not os.path.exists(csv_path):
            continue
        if not os.path.exists(properties_path):
            os.mkdir(properties_path)
        csv_files = os.listdir(csv_path)
        for i in csv_files:
            csv = os.path.join(dir_path, 'csv', i)
            subcommand = i.split('.')[0]
            generate_properties(csv, subcommand, dir_path)
            if subcommand == 'import':
                generate_properties(csv, 'export', dir_path)
                generate_properties(csv, 'generate', dir_path)


def generate_properties(csv_path, subcommand, save_path):
    subcommand_dict = {'check': (CheckDataPropertyInfo, GenerateCheckDataProperties),
                       'import': (MigrateDataPropertyInfo, GenerateMigratedDataProperties),
                       'export': (MigrateDataPropertyInfo, GenerateMigratedDataProperties),
                       'to_ods': (ToOdsPropertyInfo, GenerateToOdsProperties),
                       'generate': (MigrateDataPropertyInfo, GenerateTableDDLProperties)}
    PropertyInfoClass, GeneratePropertiesClass = subcommand_dict[subcommand]
    command_properties = PropertyObjects(csv_path, PropertyInfoClass)
    property_info_list = command_properties.generate_obj()
    properties = GeneratePropertiesClass(property_info_list, save_path)
    properties.write_properties(subcommand)


def get_directories(config_dir):
    for directory in os.listdir(config_dir):
        path_join = os.path.join(config_dir, directory)
        if os.path.isdir(path_join):
            yield path_join


if __name__ == '__main__':
    config_dir = sys.argv[1]
    main(config_dir)