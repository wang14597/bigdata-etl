import os
import re

try:
    from src.file_utils import FileUtils
except:
    from file_utils import FileUtils


class BaseGenerateProperties:
    SPLIT = """----------\n"""

    def __init__(self, property_info_list, save_path):
        self.property_info_list = property_info_list
        self.save_path = save_path

    def format_properties(self, result):
        new_result = []
        pattern = re.compile(r'^[\w |.]+=$')
        for lines in result:
            new_lines = [line for line in lines.split('\n') if not pattern.match(line)]
            new_result.append('\n'.join(new_lines))
        return new_result


class GenerateMigratedDataProperties(BaseGenerateProperties):
    TEMPLATE = FileUtils.read_only('../template/migrate/data_migrate_template')

    def get_properties(self, subcommand):
        result = []
        for property_info in self.property_info_list:
            if property_info.datetime_field != '':
                if property_info.start == '' and property_info.end != '':
                    where_condition = f'{property_info.datetime_field} <= \'{property_info.end}\''
                elif property_info.start != '' and property_info.end == '':
                    where_condition = f'{property_info.datetime_field} > \'{property_info.start}\''
                else:
                    where_condition = f'{property_info.datetime_field} > \'{property_info.start}\' and {property_info.datetime_field} <= \'{property_info.end}\''
            else:
                where_condition = ''

            if subcommand == 'import':
                source_db = property_info.db_name
                source_table = property_info.table_name
                db_type = property_info.db_type
                target_db = 'data_migration'
                target_table = property_info.table_name.lower()
                task_num = property_info.task_num
                split_by = property_info.split_by
            else:
                source_db = 'data_migration'
                source_table = property_info.table_name.lower()
                db_type = 'postgres'
                target_db = property_info.target_db
                target_table = property_info.target_table
                if property_info.task_num != '':
                    task_num = int(int(property_info.task_num) / 4) if int(
                        property_info.task_num) > 4 else property_info.task_num
                else:
                    task_num = property_info.task_num
                split_by = ''
            import_format = self.TEMPLATE.format(source_db, subcommand, db_type, source_table, where_condition,
                                                 target_db, target_table, task_num, split_by)
            result.append(import_format)
        return result

    def write_properties(self, subcommand):
        result = self.get_properties(subcommand)
        new_result = self.format_properties(result)
        properties = self.SPLIT.join(new_result)
        FileUtils.write_file(os.path.join(self.save_path, f'properties/sqoop_{subcommand}.properties'),
                             properties)


class GenerateCheckDataProperties(BaseGenerateProperties):
    TEMPLATE = FileUtils.read_only('../template/check/check_template')

    def get_properties(self):
        result = []
        for property_info in self.property_info_list:
            if property_info.datetime_field != '':
                if property_info.start == '' and property_info.end != '':
                    where_condition = f'where "{property_info.datetime_field}" <= \'{property_info.end}\''
                elif property_info.start != '' and property_info.end == '':
                    where_condition = f'where "{property_info.datetime_field}" > \'{property_info.start}\''
                elif property_info.start != '' and property_info.end != '':
                    where_condition = f'where "{property_info.datetime_field}" > \'{property_info.start}\' and "{property_info.datetime_field}" <= \'{property_info.end}\''
                else:
                    where_condition = ''
            else:
                where_condition = ''

            import_format = self.TEMPLATE.format(property_info.db_name, property_info.table_name, 'dbo',
                                                 where_condition, 'cdp_'+property_info.target_db, property_info.target_table,
                                                 property_info.target_db, property_info.datetime_field,
                                                 property_info.exclusive_columns,
                                                 property_info.datetime_field, property_info.duration,
                                                 property_info.numeric_precision)
            result.append(import_format)
        return result

    def write_properties(self, subcommand):
        result = self.get_properties()
        new_result = self.format_properties(result)
        properties = self.SPLIT.join(new_result)
        FileUtils.write_file(os.path.join(self.save_path, f'properties/{subcommand}.properties'),
                             properties)


class GenerateToOdsProperties(BaseGenerateProperties):
    TEMPLATE = FileUtils.read_only('../template/migrate/to_ods_template')

    def get_properties(self):
        result = []
        for property_info in self.property_info_list:
            import_format = self.TEMPLATE.format(property_info.db_name, property_info.table_name,
                                                 property_info.target_db, property_info.target_table,
                                                 property_info.partition_datetime, property_info.partition,
                                                 property_info.columns_relationship)
            result.append(import_format)
        return result

    def write_properties(self, subcommand):
        result = self.get_properties()
        new_result = self.format_properties(result)
        properties = self.SPLIT.join(new_result)
        FileUtils.write_file(os.path.join(self.save_path, f'properties/{subcommand}.properties'),
                             properties)


class GenerateTableDDLProperties(BaseGenerateProperties):
    TEMPLATE = FileUtils.read_only('../template/generate/generate_template')

    def get_properties(self):
        tables = []
        info = self.property_info_list[0]
        source_db = info.db_name
        for property_info in self.property_info_list:
            tables.append(property_info.table_name)
        source_tables = ','.join(tables)
        hive_ddl = self.TEMPLATE.format(source_db, source_tables, 'hive', '', 'data_migration', '')
        yb_ddl = self.TEMPLATE.format(source_db, source_tables, 'postgres', 'bigdata_prod', 'cdp_' + source_db, 'true')
        return [hive_ddl, yb_ddl]

    def write_properties(self, subcommand):
        result = self.get_properties()
        new_result = self.format_properties(result)
        FileUtils.write_file(os.path.join(self.save_path, f'properties/{subcommand}_hive.properties'),
                             new_result[0])
        FileUtils.write_file(os.path.join(self.save_path, f'properties/{subcommand}_yb.properties'),
                             new_result[1])
