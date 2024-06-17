# documentation
This repo contains the python code to generate oozie workflow and the configuration files 
for data migration. It also includes with a simple .gitlab-ci.yml which compress the job folder into a zip.

## usage
required python version >= 3.8 </br>
no additional 
```
sh main.sh
```
It will generate a workflow.xml for each folder under tmp/data-migration-config/{systemName}
and a whole xml containing all workflow.json under the project root.
<br>

This program loop all the folder under tmp/data-migration-config, and regenerate a workflow
using <b>workflow-template.json</b>

NOTICE:
    A new uuid will be generated if there is workflow.json under the system(ie: transportationstaging) folder, so if we just
want to update an existing workflow instead create a new one remember to keep the old one in the folder. 

Detail can be found in [documentation.md](documentation.md)