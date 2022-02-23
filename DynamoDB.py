import json
import boto3
from botocore.exceptions import ClientError


def create_movie_table():
    db = boto3.resource('dynamodb')
    table = db.create_table(
        TableName='fc_log',
        KeySchema=[
            {
                'AttributeName': 'fc_name',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'duration',
                'KeyType': 'RANGE'  # Sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'fc_name',
                'AttributeType': 'S'  # String
            },
            {
                'AttributeName': 'duration',
                'AttributeType': 'N'  # Number
            },
        ]
    )
    return table


def put_fc_log():
    db = boto3.resource('dynamodb')
    table = db.Table('cloud_fc_log')

    for table in db.tables.all():
        print(table.name)

    try:
        table.put_item(
            Item={
                'request_id': "00000805-fa5f-4449-8625-102b4876aed5",
                'duration': 57468,
                'cloud_type': 2,
                'fc_svc_name': 'FinancialEngineering',
                'fc_name': 'hmm_10d_test',
                'memory': 512,
                'remark': 'test',
                'start_time': 1645610352,
                'end_time': 1645610360

            }
        )
    except ClientError as e:
        print(e.response['Error']['Message'])


def lambda_handler(event, context):
    # movie_table = create_movie_table()
    # print("Table status:", movie_table.table_status)

    put_fc_log()
    print("Put movie succeeded:")


    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
