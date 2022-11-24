from etl import ETL


# 需要修改配置的地方===================================================

# 需要运维同学创建一个 SQS。 sqs的url地址。 和 file_listener.py中的是同一个
SQS_RUL = "https://sqs.cn-northwest-1.amazonaws.com.cn/xxxxxxxx/demo"

# 在athena web 界面能看到相关配置信息
ATHENA_CTX = {'Database': 'default', 'Catalog': 'AwsDataCatalog'}
ATHENA_OUTPUT_CONF = {'OutputLocation': 's3://xxxxxxxxx-cn-northwest-1/'}
ATHENA_WORK_GROUP = 'primary'

# 存放原始日志文件的S3信息
TEMP_TABLE_S3_BUCKET = 'example-output'
TEMP_TABLE_S3_PREFIX = 'demo_temp'

# 生成的临时表目录
TEMP_TABLE_NAME_PREFIX = "temp_log_athena"

# 用于记录状态信息的表
DYNAMODB_STATUS_TABLE = 'log_task_status_monitor'
DYNAMODB_STATUS_TABLE_KEY = 'keyetag'

# ===================================================================


class DemoETL(ETL):
    def __init__(self, sqs_url: str, athena_data_ctx: dict, athena_output_cfg: dict, athena_work_group: str, temp_table_name_prefix: str,
                 temp_table_s3_bucket: str, temp_table_s3_prefix: str, dynamodb_status_table: str, dynamodb_status_table_key: str,
                 concurrency: int = 10, bath_task_count: int = 1000, bath_wait_seconds: int = 15):

        super().__init__(sqs_url, athena_data_ctx, athena_output_cfg, athena_work_group, temp_table_name_prefix,
                         temp_table_s3_bucket, temp_table_s3_prefix, dynamodb_status_table, dynamodb_status_table_key,
                         concurrency, bath_task_count, bath_wait_seconds)

    def get_task_meta(self, file_key: str):
        items = file_key.split("/")
        file_name = items[-1]
        create_dt = "".join(items[-4:-1])
        log_project = items[-6]
        logstore_name = items[-5]

        # 相同的业务逻辑,日志schema一致，使用同一个group key
        group_key = f"{create_dt}/{log_project}/{logstore_name}"

        return group_key, {
            "file_name": file_name,
            "file_key": file_key
        }

    def get_etl_sql(self, group_key: str, source_table: str) -> str:
        # 由于不同的 group_key, 指向不同的日志schema,所以编写的etl sql一般不同，sql一般可以利用 json_extract_scalar 函数从json text提前关键信息用来提速查询

        parts = group_key.split("/")
        create_dt = parts[0]
        log_project = parts[1]
        logstore_name = parts[2]
        business_type = parts[3]
        # 目标表的create_dt 的数据类型为字符串
        sql = f"""
                    insert into demo_athena_table1
                    select json_text
                        , json_extract_scalar(json_text, '$.__time__') as unix_time
                        , json_extract_scalar(json_text, '$.client_ip'), '')  as ip
                        , '{log_project}' as log_project
                        , '{logstore_name}' as logstore_name
                        , '{business_type}' as business_type
                        , '{create_dt}' as create_dt
                    from {source_table}
                    """

        return sql


demo = DemoETL(
    sqs_uri=SQS_RUL,
    athena_data_ctx=ATHENA_CTX,
    athena_output_cfg=ATHENA_OUTPUT_CONF,
    athena_work_group=ATHENA_WORK_GROUP,
    temp_table_name_prefix=TEMP_TABLE_NAME_PREFIX,
    temp_table_s3_bucket=TEMP_TABLE_S3_BUCKET,
    temp_table_s3_prefix=TEMP_TABLE_S3_PREFIX,
    dynamodb_status_table=DYNAMODB_STATUS_TABLE,
    dynamodb_status_table_key=DYNAMODB_STATUS_TABLE_KEY
)

demo.run()
