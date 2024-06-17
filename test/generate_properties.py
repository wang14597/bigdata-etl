import unittest

from src.generate_properties.generate_tools import GenerateMigratedDataProperties
from src.generate_properties.model import *


class GenerateProperties(unittest.TestCase):

    args = [i.strip() for i in 'a,a_1,LOAD_DT,2021-01-01 00:00:00,2021-01-01 00:00:00,A,null,null,day'.split(',')]
    csv_path = '../src/resource/template_test.csv'

    def test_generate_property_info(self):
        property_info = MigrateDataPropertyInfo(*self.args)
        info_dir = tuple(property_info.__dir__())
        print(info_dir)
        for i in range(0,len(info_dir)):
            self.assertEqual(self.args[i], getattr(property_info, info_dir[i]))

    def test_read_from_csv(self):
        expected = 'db_name,table_name,datetime_field,start,end,target_db,target_table,excepted,partition'
        command_properties = PropertyObjects(self.csv_path, MigrateDataPropertyInfo)
        line = command_properties.get_csv_lines()[0]
        self.assertEqual(line, expected)

    def test_generate_property_info_instance(self):
        command_properties = PropertyObjects(self.csv_path, MigrateDataPropertyInfo)
        obj = command_properties.generate_obj()[0]
        self.assertEqual(obj.__str__(),'a,a_1,LOAD_DT,2021-01-01 00:00:00,2021-01-01 00:00:00,A,,,day')

    def test_generate_migrated_data_property(self):
        expected = """sourceDB=a
subcommand=import
dbType=sql_server
sourceTable=a_1
derivedExpr=year(LOAD_DT) as year,month(LOAD_DT) as month,day(LOAD_DT) as day
whereCondition=LOAD_DT > '2021-01-01 00:00:00' and LOAD_DT <= '2021-01-01 00:00:00'
targetDB=A
targetTable=
excepted=
"""
        command_properties = PropertyObjects(self.csv_path, MigrateDataPropertyInfo)
        objs = command_properties.generate_obj()
        migrated_data_properties = GenerateMigratedDataProperties(objs, None)
        properties_str = migrated_data_properties.get_properties('import')[0]
        self.assertEqual(properties_str, expected)

    @unittest.skip
    def test_write_properties(self):
        command_properties = PropertyObjects(self.csv_path, MigrateDataPropertyInfo)
        objs = command_properties.generate_obj()
        migrated_data_properties = GenerateMigratedDataProperties(objs, None)
        migrated_data_properties.write_properties('import')


if __name__ == '__main__':
    unittest.main()
