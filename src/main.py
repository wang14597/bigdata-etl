import sys
from file_utils import get_template_json
from workflow_class import *


def workflow_factory(workflow_type, config_dir, jar_path):
    template_str = get_template_json(workflow_type)
    if workflow_type == "generate":
        return WorkflowGeneratorCreateTable(
            workflow_template=template_str[0],
            workflow_data_template=template_str[1],
            workflow_data_workflow_template=template_str[2],
            config_dir=config_dir,
            jar_path=jar_path, job_type="generate-table")

    if workflow_type == "migrate":
        return WorkflowGeneratorDataMigrationUseSpark(
            workflow_template=template_str[0],
            workflow_data_template=template_str[1],
            workflow_data_workflow_template=template_str[2],
            config_dir=config_dir,
            jar_path=jar_path, job_type="data-migration")

    if workflow_type == "check":
        return WorkflowGeneratorCheck(
            workflow_template=template_str[0],
            workflow_data_template=template_str[1],
            workflow_data_workflow_template=template_str[2],
            config_dir=config_dir,
            jar_path=jar_path, job_type="data-check")


def main(config_dir, jar_path):
    workflow_type = ("generate", "migrate", "check")
    workflow = []
    for i in workflow_type:
        work_flow_obj = workflow_factory(i, config_dir, jar_path)
        work_flow_obj.generate()
        workflow.extend(work_flow_obj.wrapper)
    save_path = os.path.join(os.path.abspath("."), "oozie-workflow.json")
    FileUtils.write_file(save_path, json.dumps(workflow))


if __name__ == '__main__':
    config_dir, jar_path = sys.argv[1], sys.argv[2]
    main(config_dir, jar_path)
    # main("/Users/lei.wang2/PycharmProjects/data-migration-workflow/conf/data-migration-config", "/user/z3499/code.jar")




