import json
import os
from uuid import uuid4
from file_utils import FileUtils
from datetime import datetime


class WorkFlowGeneratorBase:

    def __init__(
            self,
            workflow_template,
            workflow_data_template,
            workflow_data_workflow_template,
            config_dir,
            jar_path,
            job_type

    ):
        self.workflow_template = workflow_template
        self.workflow_data_template = workflow_data_template
        self.workflow_data_workflow_template = workflow_data_workflow_template
        self.nodeNameIdMap = dict()
        self.config_dir = config_dir
        self.jar_path = jar_path
        self.__generate_node(workflow_data_template)
        self.wrapper = list()
        self.job_type = job_type

    def __generate_node(self, workflow_data_template):
        for layout in workflow_data_template["layout"]:
            for row in layout["oozieRows"]:
                for widget in row["widgets"]:
                    self.nodeNameIdMap[widget["id"]] = widget["name"]

    def generate(self):
        for file in os.listdir(self.config_dir):
            self.generate_workflow_for_system(file)

    def generate_workflow_for_system(self, system_name: str):
        oozie_workflow = self.workflow_data_workflow_template
        hue_oozie_layout = self.workflow_data_template
        hue_workflow = self.workflow_template
        # set the workflow name, the format is data-migration-${systemZName}
        job_name = self.job_type + "-" + system_name
        hue_workflow["fields"]["name"] = job_name
        oozie_workflow['name'] = job_name
        workflow_json_path = os.path.join(os.path.abspath("."),
                                          self.config_dir,
                                          system_name,
                                          f"{job_name}-workflow.json")
        uuid = str(uuid4())
        # using the old uuid if there is a workflow.json under the folder
        if os.path.exists(workflow_json_path):
            existed = FileUtils.read_only(workflow_json_path)
            uuid = json.loads(existed)[0]["fields"]['uuid']

        self.handle_nodes(oozie_workflow, system_name)

        oozie_workflow["uuid"] = uuid
        hue_oozie_layout['workflow'] = oozie_workflow
        hue_workflow['fields']['data'] = json.dumps(hue_oozie_layout)
        hue_workflow["fields"]['uuid'] = uuid
        hue_workflow["fields"]["last_modified"] = datetime.strftime(datetime.now(), '%Y-%m-%dT%H:%M:%S')

        FileUtils.write_file(workflow_json_path, json.dumps([hue_workflow]))
        self.wrapper.append(hue_workflow)

    def handle_nodes(self, oozie_workflow, system_name):
        raise NotImplementedError("handle_nodes did not implemented!")


class WorkflowGeneratorDataMigration(WorkFlowGeneratorBase):

    def handle_sqoop_import_node(self, node, system):
        base_dir = os.path.join("hdfs:///etl", self.config_dir, system)
        sqoop_import_params = ["sqoop", "-c", os.path.join(base_dir, "properties", "sqoop_import.properties"), "--",
                               "outPath=" + os.path.join(base_dir, "sqoop_import.log")]
        mapped = map(lambda para: {'value': para}, sqoop_import_params)
        node['properties']['arguments'] = list(mapped)
        node['properties']['jar_path'] = self.jar_path

    def handle_yb_export_node(self, node, system):
        base_dir = os.path.join("hdfs:///etl", self.config_dir, system)
        yb_import_params = ["sqoop", "-c", os.path.join(base_dir, "properties", "yb_export.properties"), "--",
                            "outPath=" + os.path.join(base_dir, "sqoop_export.log")]
        mapped = map(lambda para: {'value': para}, yb_import_params)
        node['properties']['arguments'] = list(mapped)
        node['properties']['jar_path'] = self.jar_path

    def handle_to_ods_node(self, node, system):
        base_dir = os.path.join("hdfs:///etl", self.config_dir, system)
        to_ods_params = ["to_ods", "-c", os.path.join(base_dir, "properties", "to_ods.properties"), "--",
                         "outPath=" + os.path.join(base_dir, "to_ods.log")]
        mapped = map(lambda para: {'value': para}, to_ods_params)
        node['properties']['arguments'] = list(mapped)
        node['properties']['jar_path'] = self.jar_path

    def handle_nodes(self, oozie_workflow, system_name):
        for node in oozie_workflow['nodes']:
            if node['id'] in self.nodeNameIdMap and self.nodeNameIdMap[node['id']] == 'sqoop_import':
                self.handle_sqoop_import_node(node, system_name)
            elif node['id'] in self.nodeNameIdMap and self.nodeNameIdMap[node['id']] == 'yb_export':
                self.handle_yb_export_node(node, system_name)
            elif node['id'] in self.nodeNameIdMap and self.nodeNameIdMap[node['id']] == 'to_ods':
                self.handle_to_ods_node(node, system_name)


class WorkflowGeneratorCreateTable(WorkFlowGeneratorBase):

    def handle_nodes(self, oozie_workflow, system_name):
        for node in oozie_workflow['nodes']:
            if node['id'] in self.nodeNameIdMap and self.nodeNameIdMap[node['id']] == 'generate_hive':
                self.handle_generate_node(node, system_name, "Hive")
            elif node['id'] in self.nodeNameIdMap and self.nodeNameIdMap[node['id']] == 'generate_yb':
                self.handle_generate_node(node, system_name, "Yb")
            elif node['id'] in self.nodeNameIdMap and self.nodeNameIdMap[node['id']] == 'create_hive_table':
                self.handle_create_hive_table(node, system_name)

    def handle_generate_node(self, node, system_name, generate_type):
        base_dir = os.path.join("hdfs:///etl", self.config_dir, system_name)
        params = ["generate", "-c",
                  os.path.join(base_dir, "properties", f"generate_{generate_type.lower()}.properties"), "-p",
                  os.path.join(base_dir, f"{generate_type}_{system_name}.sql")]
        mapped = map(lambda para: {'value': para}, params)
        node['properties']['arguments'] = list(mapped)
        node['properties']['jar_path'] = self.jar_path

    def handle_create_hive_table(self, node, system_name):
        base_dir = os.path.join("hdfs:///etl", self.config_dir, system_name)
        node['properties']["script_path"] = f"{base_dir}/Hive_{system_name}.sql"


class WorkflowGeneratorCheck(WorkFlowGeneratorBase):

    def handle_nodes(self, oozie_workflow, system_name):
        for node in oozie_workflow['nodes']:
            if node['id'] in self.nodeNameIdMap and self.nodeNameIdMap[node['id']] == 'check':
                self.handle_check_node(node, system_name)

    def handle_check_node(self, node, system_name):
        base_dir = os.path.join("hdfs:///etl", self.config_dir, system_name)
        params = ["check", "-p", os.path.join(base_dir, "properties", f"check.properties")]
        mapped = map(lambda para: {'value': para}, params)
        node['properties']['spark_arguments'] = list(mapped)
        node['properties']['files'][0]['value'] = self.jar_path
        node['properties']['jars'] = self.jar_path.split("/")[-1]


class WorkflowGeneratorDataMigrationUseSpark(WorkFlowGeneratorBase):

    def handle_nodes(self, oozie_workflow, system_name):
        for node in oozie_workflow['nodes']:
            if node['id'] in self.nodeNameIdMap and self.nodeNameIdMap[node['id']] == 'sqoop_import':
                self.handle_sqoop_import_node(node, system_name)
            elif node['id'] in self.nodeNameIdMap and self.nodeNameIdMap[node['id']] == 'sqoop_export':
                self.handle_yb_export_node(node, system_name)
            elif node['id'] in self.nodeNameIdMap and self.nodeNameIdMap[node['id']] == 'to_ods':
                self.handle_to_ods_node(node, system_name)

    def handle_sqoop_import_node(self, node, system):
        base_dir = os.path.join("hdfs:///etl", self.config_dir, system)
        sqoop_import_params = ["sqoop", "-c", os.path.join(base_dir, "properties", "sqoop_import.properties"), "--",
                               "outPath=" + os.path.join(base_dir, "sqoop_import.log")]
        mapped = map(lambda para: {'value': para}, sqoop_import_params)
        node['properties']['spark_arguments'] = list(mapped)
        node['properties']['files'][0]['value'] = self.jar_path
        node['properties']['jars'] = self.jar_path.split("/")[-1]

    def handle_yb_export_node(self, node, system):
        base_dir = os.path.join("hdfs:///etl", self.config_dir, system)
        sqoop_import_params = ["sqoop", "-c", os.path.join(base_dir, "properties", "sqoop_export.properties"), "--",
                               "outPath=" + os.path.join(base_dir, "sqoop_export.log")]
        mapped = map(lambda para: {'value': para}, sqoop_import_params)
        node['properties']['spark_arguments'] = list(mapped)
        node['properties']['files'][0]['value'] = self.jar_path
        node['properties']['jars'] = self.jar_path.split("/")[-1]

    def handle_to_ods_node(self, node, system):
        base_dir = os.path.join("hdfs:///etl", self.config_dir, system)
        sqoop_import_params = ["to_ods", "-c", os.path.join(base_dir, "properties", "to_ods.properties"), "--",
                               "outPath=" + os.path.join(base_dir, "to_ods.log")]
        mapped = map(lambda para: {'value': para}, sqoop_import_params)
        node['properties']['spark_arguments'] = list(mapped)
        node['properties']['files'][0]['value'] = self.jar_path
        node['properties']['jars'] = self.jar_path.split("/")[-1]
