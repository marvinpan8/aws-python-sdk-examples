import json
import boto3
from botocore.exceptions import ClientError


def create_movie_table():
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.create_table(
        TableName='fc_log',
        KeySchema=[
            {
                'AttributeName': 'request_id',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'duration',
                'KeyType': 'RANGE'  # Sort key
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'request_id',
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
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('cloud_fc_log')

    for table in dynamodb.tables.all():
        print(table.name)

    try:
        response = table.put_item(
            Item={
                'request_id': "00000805-fa5f-4449-8625-102b4876aed5",
                'duration': 57468,
                'cloud_type': 2,
                'fc_svc_name': 'FinancialEngineering',
                'fc_name': 'hmm_10d_test',
                'memory': 512,
                'remark': 'test',
                'exec_time': '2022-02-10 18:08:00',
                'created_time': '2022-02-10 18:08:58'

            }
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return response['Item']

    return response


def lambda_handler(event, context):
    # movie_table = create_movie_table()
    # print("Table status:", movie_table.table_status)

    put_fc_log()
    print("Put movie succeeded:")


    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
