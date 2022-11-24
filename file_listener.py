import json
import urllib.parse
import boto3

# 需要修改配置的地方============================================================
sqs_url = "https://sqs.cn-northwest-1.amazonaws.com.cn/xxxxxxxx/demo"
dynamodb_status_table = 'log_task_status_monitor'
dynamodb_status_table_key = 'keyetag'
# ============================================================================


def lambda_handler(event, context):

    sqs = boto3.client("sqs")
    dynamodb = boto3.client('dynamodb')

    for record in event['Records']:
        etag = record['s3']['object']['eTag']
        verified_key = f"{record['s3']['object']['key']}/{etag}"
        dy_resp = dynamodb.get_item(
            Key={
                dynamodb_status_table_key: {
                    'S': verified_key,
                }
            },
            TableName=dynamodb_status_table,
        )
        if 'Item' not in dy_resp or dy_resp['Item']['status']['S'] == 'failure':
            event_time = record['eventTime']
            dynamodb.update_item(
                TableName=dynamodb_status_table,
                Key={
                    dynamodb_status_table_key: {'S': verified_key},

                },
                AttributeUpdates={
                    'status': {
                        'Value':  {
                            "S": "begin"
                        }
                    },
                    'event_time': {
                        'Value':  {
                            "S": event_time
                        }
                    },
                    'error': {
                        'Value':  {
                            "S": '-'
                        }
                    }
                }
            )
            key = f"{record['s3']['bucket']['name']}/{record['s3']['object']['key']}"
            ukey = urllib.parse.unquote_plus(key, encoding='utf-8')
            k_info = {
                "file_key": ukey,
                "verified_key": verified_key,
                "event_time": event_time
            }
            str_info = json.dumps(k_info)
            sqs_resp = sqs.send_message(
                QueueUrl=sqs_url,
                MessageBody=str_info,
                DelaySeconds=0)
            print(sqs_resp)

    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
