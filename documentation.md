# 根据CSV文件生成`etl-tools`工具所需要的配置文件和workflow

## 1、目录结构
```
conf
└── data-migration-config
    └── transportationstaging
        ├── csv
        │   ├── check.csv
        │   ├── import.csv
        │   └── to_ods.csv
        ├── properties
        │    ├── check.properties
        │    ├── generate_hive.properties
        │    ├── generate_yb.properties
        │    ├── sqoop_export.properties
        │    ├── sqoop_import.properties
        │    └── to_ods.properties
        ├── data-check-transportationstaging-workflow.json
        ├── data-migration-transportationstaging-workflow.json
        └── generate-table-transportationstaging-workflow.json
```
| 目录 | 说明 |
| :----: | :----: |
| `conf` | 配置信息根目录 |
| `data-migration-config` | 子目录包含个系统或数据库文件夹 |
| `transportationstaging` | 系统或数据库名，包含csv配置信息和生成的properties以及workflow|
| `csv` | 包含`check.csv`,`import.csv`,`to_ods.csv`|
| `properties` | 根据`csv`文件夹下的csv文件生成的配置文件|
| `data-check-transportationstaging-workflow.json` | data check的workflow|
| `data-migration-transportationstaging-workflow.json` | 数据迁移的workflow|
| `generate-table-transportationstaging-workflow.json` | 生成表结构的workflow|
| `check.csv` | 生成`check.properties`|
| `import.csv` | 生成`generate_hive.properties`,`generate_yb.properties`,`sqoop_export.properties`,`sqoop_import.properties`|
| `to_ods.csv` | 生成`to_ods.properties`|

## 2、使用

每迁移一个系统或数据库，需在`data-migration-config`文件夹下创建该系统或数据库的子目录，以quantum中`transportationstaging`数据库为例：

- step1：创建`transportationstaging`文件夹
- step2：在`transportationstaging`文件夹创建`csv`文件夹
- step3：在`csv`文件夹中分别编写`import.csv`,`to_ods.csv`,`check.csv`三个配置文件
- step4：执行`sh main.sh`,将自动生成`properties`文件夹和各配置文件，同时在`transportationstaging`目录下创建3个workflow

## 3、csv文件配置介绍

### 3.1、import.csv
该csv文件用于生成：

- `generate_hive.properties`: 生成hive表结构
- `generate_yb.properties`：生成yb表结构
- `sqoop_import.properties`：将quantum中的数据迁移至hive过渡表中（hive过渡表默认与quantum表名相同,库名为`data_migration`）
- `sqoop_export.properties`：将hive过渡表中数据迁移至YB数据库中

|db_type|db_name|table_name|datetime_field|start|end|target_db|target_table|taskNum|splitBy|
| :----:| :----: | :----: | :----: | :----: | :----: | :----: | :----: | :----: | :----: |
|数据库类型|数据库名|表名|表中业务时间字段|开始时间|结束时间|目标数据库（yb）|目标表|map节点数量（根据表数据量设置，大致关系为1g对应一个map）|数据切分字段（主键或索引（业务时间索引等），都无可取业务时间字段）|
以quantum中`transportationstaging`数据库中表`E_LIMO_CURR_BookingQuery`为例：

```
transportationstaging.dbo.E_LIMO_CURR_BookingQuery -> data_migration.e_limo_curr_Bookingquert -> cdp_transportationstaging.E_LIMO_CURR_BookingQuery
（quantum -> hive过渡 -> yb）
假设数据量1.5G，设置taskNum=2，HIST_DT为业务时间字段，splitBy取HIST_DT
```
csv配置如下：

|db_type|db_name|table_name|datetime_field|start|end|target_db|target_table|taskNum|splitBy|
| :----:| :----: | :----: | :----: | :----: | :----: | :----: | :----: | :----: | :----: |
|sql_server|transportationstaging|E_LIMO_CURR_BookingQuery|NULL|NULL|NULL|cdp_transportationstaging|E_LIMO_CURR_BookingQuery|2|HIST_DT|

### 3.2、to_ods.csv
该csv文件用于生成：

- `to_ods.properties`: hive过渡表 -> hive ods表

|db_name|table_name|partitionDate|target_db|target_table|partition|derivedColumn|
| :----: | :----: | :----: | :----: | :----: | :----: | :----: |
|hive过渡数据库名|过渡表名|初始化时间（第一次迁移数据进ods层时间当作分区）|ods数据库名|ods表名|分区粒度day/hour，或者传入自定义分区，同时传入分区值，如year='2021',month='02',day='01'|派生列信息，可指定列值，如 'applicationId_10001' as \`job_id\`,'migrate_to_ods' as \`job_name\`|

以quantum中`transportationstaging`数据库中表`E_LIMO_CURR_BookingQuery`为例，假设其在hive中存在ods表`ods_e_limo_curr_Bookingquert`：

3.1已经将数据迁移到了hive过渡表中`data_migration.e_limo_curr_Bookingquert`，此时需要将该过渡表数据导入ods表，以天为分区，默认导入到`2022-01-01`分区。

|db_name|table_name|partitionDate|target_db|target_table|partition|derivedColumn|
| :----: | :----: | :----: | :----: | :----: | :----: | :----: | 
|data_migration|e_limo_curr_Bookingquert|2022-01-01 00:00:00|transportationstaging|ods_e_limo_curr_Bookingquert|day|NULL|

### 3.3、check.csv
该csv文件用于生成：

- `check.properties`: check quantum -> yb 数据一致性

|db_name|table_name|datetime_field|start|end|target_db|target_table|duration|numeric_precision|exclusiveColumns|
| :----: | :----: | :----: | :----: | :----: | :----: | :----: | :----: | :----: | :----: | 
|数据库名|表名|业务时间字段|开始|结束|目标库名|目标表名|间隔|浮点数精度|排除列|

使用hash进行check的时候，按照`duration`进行分组，如`duration`为`day`，则按照业务时间字段`datetime_field`进行每天进行group by分组得到每天的一个hash值，比较两表hash值是否相同。

`numeric_precision`：设置浮点数精度，某些情况两张表同一列浮点数精度不同，如sql server为 0.1,yb为 0.100,此时hash结果将不同，需要对精度进行统一。

`exclusiveColumns`：排除哪些列不做check，多个字段以`,`分割

### 补充：对于offline的数据库或一些没有经过hive的数据库等，可不配置to_ods.csv