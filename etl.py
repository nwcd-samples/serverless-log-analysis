from abc import ABCMeta, abstractmethod
from datetime import datetime
import json
import time
import urllib.parse
import boto3
import random


class ETL:
    __metaclass__ = ABCMeta

    def __init__(self,
                 sqs_url: str,
                 athena_data_ctx: dict,
                 athena_output_cfg: dict,
                 athena_work_group: str,
                 temp_table_name_prefix: str,
                 temp_table_s3_bucket: str,
                 temp_table_s3_prefix: str,
                 dynamodb_status_table: str,
                 dynamodb_status_table_key: str,
                 concurrency: int = 10,
                 bath_task_count: int = 1000,
                 bath_wait_seconds: int = 15):
        self.sqs_url = sqs_url
        self.athena_ctx = athena_data_ctx
        self.athena_output_cfg = athena_output_cfg
        self.athena_work_group = athena_work_group
        self.temp_table_s3_bucket = temp_table_s3_bucket
        self.temp_table_name_prefix = temp_table_name_prefix
        self.temp_table_s3_prefix = temp_table_s3_prefix
        self.dynamodb_status_table = dynamodb_status_table
        self.dynamodb_status_table_key = dynamodb_status_table_key
        self.concurrency = concurrency
        self.bath_task_count = bath_task_count
        self.bath_wait_seconds = bath_wait_seconds

        # 临时表字典
        self.temp_tables = dict()
        self.temp_tables_s3 = dict()
        pass

    def run(self):
        self.__setup()
        print("begin")
        while True:
            s3 = boto3.client("s3")
            athena = boto3.client('athena')
            dynamodb = boto3.client('dynamodb')

            # 一次获取批量的任务
            batch_tasks = self.__get_tasks(
                self.bath_task_count, self.bath_wait_seconds)
            print(f"got============> {len(batch_tasks)} tasks")
            # 把一批任务根据不同的分区键，分成多个小批量任务
            task_router, bad_tasks = self.__split_tasks(batch_tasks)
            # print(task_router)
            print(f"got============> {len(task_router)} task type")

            sqls = list()
            # 生成的sql 和 group key的隐射关系
            sql_group_key_map = dict()
            # 复制s3文件时，文件和错误直接的关系
            copy_errors = dict()
            # 这一个批次任务用到了那些临时表，以及放入临时表的文件，后续通过这个删除这些临时文件
            using_temp_table = dict()

            # 对子批量任务进行获取空闲临时表，复制文件，插入，记录状态，删除文件，上次文件同步状态，释放临时表
            for group_key in task_router:

                temp_tb = self.__pop_avalible_temp_table()

                t_files, e = self.__copy_goup_files(
                    task_router, group_key, temp_tb, s3)
                for item in e:
                    copy_errors[item] = f"failed to copy {item} to temp table: {temp_tb}"
                using_temp_table[temp_tb] = t_files

                sql = self.get_etl_sql(group_key, temp_tb)

                sqls.append(sql)
                sql_group_key_map[sql] = group_key

            print("begin  query")
            query_errors = self.__query_batch(athena, sqls)

            # 先将所有的文件状态改为成功
            all_files = [meta["file_key"]
                         for key in task_router for meta in task_router[key]]

            # 删除临时文件
            for temp_tp in using_temp_table:
                files = using_temp_table[temp_tp]
                self.__delete_s3_files(s3, files)
                time.sleep(0.1)
                # 释放临时表
                self.__release_temp_table(temp_tp)
                print(f"release temp table {temp_tp}")

            print("done delete files and release temp tables")

            for t_file_key in all_files:
                self.__update_file_status(dynamodb, t_file_key, "success")

            # 在把复制失败的错误的文件状态改成失败
            for copy_error_file in copy_errors:
                self.__update_file_status(dynamodb, copy_error_file,
                                          "failure", copy_errors[copy_error_file])

            # 在把查询错误的文件状态改成失败
            if query_errors:
                for item in query_errors:
                    error = query_errors[item]
                    sql_group_key = sql_group_key_map[item]
                    relative_files = [meta["file_key"]
                                      for meta in task_router[sql_group_key]]

                    for file_key in relative_files:
                        self.__update_file_status(
                            dynamodb, file_key, "failure", error)

            time.sleep(0.5)

    @abstractmethod
    def get_etl_sql(group_key: str, temp_table: str) -> str:
        raise ValueError("need implemented in subclass")

    @abstractmethod
    def get_task_meta(file_key: str) -> tuple(str, dict):
        """
        return two object, task group key and file inforamtion dict, eg:

        group_key, {
        "file_name": file_name,
        "file_key": file_key
        }

        group_key is string , it depends on your business logic

        {
            file_name: is the name of log file
            file key: is the s3 path of file
        }
        """
        raise ValueError("need implemented in subclass")

    def __setup(self):
        # 生成临时表
        # 生成一个随机字符，作为临时表的后缀，防止如果跑多个程序，临时表被互相抢占
        chars = "abcdefghlgklmnopqrstuvwxyz"
        leng = random.randint(3, 8)
        begin = random.randint(0, 26-leng-1)
        random_word = chars[begin: begin+leng]
        print(f"temp table prefix: {random_word}")
        athena = boto3.client('athena')

        sqls = list()
        for i in range(1, self.concurrency+1):
            table_name = f"{self.temp_table_name_prefix}_{random_word}{i}"
            file_s3_prefix = f"{ self.temp_table_s3_prefix}/{random_word}{i}"
            self.temp_tables[table_name] = True
            self.temp_tables_s3[table_name] = file_s3_prefix
            s3_path = f"s3://{self.temp_table_s3_bucket}/{file_s3_prefix}"

            ddl = f"""
                    CREATE EXTERNAL TABLE `{table_name}`(
                    `json_text` string COMMENT 'from deserializer')
                    ROW FORMAT SERDE
                    'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
                    STORED AS INPUTFORMAT
                    'org.apache.hadoop.mapred.TextInputFormat'
                    OUTPUTFORMAT
                    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
                    LOCATION
                    '{s3_path}/'
                    """
            sqls.append(ddl)

        errors = self.__query_batch(athena, sqls)

        if errors:
            raise ValueError(errors)

    def __query_batch(self, athena, sqls: list, timeout=300) -> dict:
        '''
        一次发起 len(sqls) 个 athena 请求，并同时轮询几个请求的执行状态，所有都执行成功了
        再统一返回。比等待时间是几个sql中执行最长的sql的时间
        '''
        executions = dict()

        for sql in sqls:
            execution = athena.start_query_execution(
                QueryString=sql,
                QueryExecutionContext=self.athena_ctx,
                ResultConfiguration=self.athena_output_cfg,
                WorkGroup=self.athena_work_group
            )
            executions[sql] = execution

        time.sleep(0.1)

        begin_time = datetime.now()
        errors = dict()

        while executions:

            item = executions.popitem()
            sql = item[0]
            execution = item[1]

            response = athena.get_query_execution(
                QueryExecutionId=execution['QueryExecutionId'])
            status = response['QueryExecution']['Status']['State']

            if status == 'SUCCEEDED':
                continue

            if status == 'FAILED':
                reason = response['QueryExecution']['Status']['StateChangeReason']
                errors[sql] = f"failed_execute_sql:[{sql}]  reason[{reason}]"

            else:
                executions[sql] = execution
                if (datetime.now() - begin_time).total_seconds() > timeout:
                    break
                time.sleep(0.5)

        while executions:
            # 这里留下来的都是由于超时，没有处理完的查询，超时的需要手动取消，并且记录下来，供人工复核
            item = executions.popitem()
            sql = item[0]
            execution = item[1]
            athena.stop_query_execution(
                QueryExecutionId=execution['QueryExecutionId'])

            errors[sql] = f"failed_execute_sql: [{sql}]  reason[timeout: {timeout} seconds]"

        return errors

    def __get_tasks(self, bach_count=1000, wait_seconds=30):
        sqs = boto3.client('sqs')

        msg_count = 0
        wait_unit_second = 0.01
        task_info = list()

        now = datetime.now()
        while True:
            response = sqs.receive_message(
                QueueUrl=self.sqs_url,
                VisibilityTimeout=60
            )

            if "Messages" in response:
                messages = response["Messages"]

                for msg in messages:
                    body_str = msg["Body"]
                    body = json.loads(body_str)
                    receipt_handle = msg["ReceiptHandle"]
                    sqs.delete_message(
                        QueueUrl=self.sqs_url,
                        ReceiptHandle=receipt_handle
                    )
                    task_info.append(body)
                    msg_count += 1

            if msg_count > bach_count:
                break

            cost = (datetime.now() - now).total_seconds()
            if cost > wait_seconds:
                break

            time.sleep(wait_unit_second)

        return task_info

    def __pop_avalible_temp_table(self) -> str:
        empty = ''
        for tb in self.temp_tables:
            if self.temp_tables[tb] == True:
                empty = tb
                break

        if empty:
            self.temp_tables[empty] = False

        return empty

    def __release_temp_table(self, tb_name):
        self.temp_tables[tb_name] = True

    def __copy_goup_files(self, task_router: dict, group_key: str, temp_tb: str, s3) -> list:
        errors = list()

        metas = task_router[group_key]
        # s3 复制参数
        # CopySource 格式 bucket/prefix/prefix/.../filename, 不需要s3://
        # Key 格式 prefix/prefix/.../filename
        # Bucket 格式 bucket  ,不需要s3://

        temp_files = list()
        for meta in metas:
            file_name = meta['file_name']
            file_key = meta['file_key']
            target_file = f"{self.temp_tables_s3[temp_tb]}/{file_name}"
            response = s3.copy_object(
                CopySource=file_key,
                Bucket=self.temp_table_s3_bucket,
                Key=target_file
            )
            if response['ResponseMetadata']['HTTPStatusCode'] != 200:
                errors.append(file_key)
            else:
                temp_files.append(target_file)
        return temp_files, errors

    def __split_tasks(self, tasks: list):
        bad_tasks = list()
        task_router = dict()

        for task in tasks:
            key = task['file_key']
            verified_key = task['verified_key']
            if not key.endswith(".gz"):
                bad_tasks.append(key)
                continue

            ukey = urllib.parse.unquote_plus(key, encoding='utf-8')

            group_key, meta = self.get_task_meta(ukey)

            if group_key not in task_router:
                task_router[group_key] = list()

            task_router[group_key].append(meta)

        return task_router, bad_tasks

    def __update_file_status(self, dynamodb, file_key, status, error: str = '-'):
        dynamodb.update_item(
            TableName=self.dynamodb_status_table,
            Key={
                self.dynamodb_status_table_key: {'S': file_key},

            },
            AttributeUpdates={
                'status': {
                    'Value':  {
                        "S": status
                    }
                },
                'error': {
                    'Value':  {
                        "S": error
                    }
                }
            }
        )

    def __delete_s3_files(self, s3,  target_files: list):
        # target_file 格式 /bucket/prefix/prefix/.../filename

        # s3 delete 参数
        # Key 格式 prefix/prefix/.../filename
        # Bucket 格式 bucket  ,不需要s3://

        o = [{
            "Key": item
        } for item in target_files]
        d = {
            'Objects': o,
            'Quiet': True
        }

        s3.delete_objects(
            Bucket=self.temp_table_s3_bucket,
            Delete=d
        )
