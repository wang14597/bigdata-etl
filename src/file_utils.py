import json
import os


class FileUtils:

    @staticmethod
    def __read_file(file_path, mode):
        with open(file_path, mode) as file:
            return file.read()

    @staticmethod
    def read_only(file_path):
        return FileUtils.__read_file(file_path, 'r')

    @staticmethod
    def write_file(file_path, content):
        with open(file_path, 'w') as file:
            file.write(content)

# not use
def get_template(type_template):
    dir_path = os.path.join(os.getcwd(), "src", "template", type_template)

    workflow_template_path = os.path.join(dir_path, "workflow-template.json")
    workflow_data_template_path = os.path.join(dir_path, "workflow-data-template.json")
    workflow_data_workflow_template_path = os.path.join(dir_path, "workflow-data-workflow-template.json")

    workflow_template_str = FileUtils.read_only(workflow_template_path)
    workflow_data_template_str = FileUtils.read_only(workflow_data_template_path)
    workflow_data_workflow_template_str = FileUtils.read_only(workflow_data_workflow_template_path)
    return workflow_template_str, workflow_data_template_str, workflow_data_workflow_template_str


def get_template_json(type_template):
    dir_path = os.path.join(os.getcwd(), "src", "template", type_template)
    # dir_path = os.path.join(os.getcwd(), "template", type_template)
    file_path = os.path.join(dir_path, "workflow-template.json")
    with open(file_path, 'r') as file:
        workflow_template = json.load(file)
    workflow_data_template = json.loads(workflow_template["fields"]["data"])
    workflow_data_workflow_template = workflow_data_template["workflow"]
    return workflow_template, workflow_data_template, workflow_data_workflow_template


def get_generate_template_properties(type_template):
    dir_path = os.path.join(os.getcwd(), "src", "template", type_template)
    hive_template_path = os.path.join(dir_path, "createHiveTableDDL.properties")
    yb_template_path = os.path.join(dir_path, "createYbTableDDL.properties")
    hive_template = FileUtils.read_only(hive_template_path)
    yb_template = FileUtils.read_only(yb_template_path)
    return hive_template, yb_template


def get_migrate_template_properties(type_template):
    dir_path = os.path.join(os.getcwd(), "src", "template", type_template)
    sqoop_import_path = os.path.join(dir_path, "sqoop_import.properties")
    yb_export_path = os.path.join(dir_path, "yb_export.properties")
    sqoop_import = FileUtils.read_only(sqoop_import_path)
    yb_export = FileUtils.read_only(yb_export_path)
    return sqoop_import, yb_export
