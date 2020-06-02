#  dependencies
import os
import boto3
import datetime

#  start dynamodb client
region = os.environ.get('AWS_DEFAULT_REGION', 'us-east-2')
client = boto3.client('dynamodb', region_name=region)

#  dynamodb table names to create
table_prefixes = ['lift-status_', 'snowfall_', 'weather_']


#  lambda function to create dynamodb table
def lambda_handler(event, context):

    today = datetime.datetime.utcnow().date()
    tomorrow = today + datetime.timedelta(1)

    for table_prefix in table_prefixes:

        #  assign table name
        new_table_name = table_prefix + tomorrow.strftime("%Y-%m-%d")

        #  create table
        client.create_table(
            TableName=new_table_name,
            KeySchema=[
                {'AttributeName': "pk", 'KeyType': "HASH"},  # Partition key
                {'AttributeName': "sk", 'KeyType': "RANGE"}  # Sort key
            ],
            AttributeDefinitions=[
                {'AttributeName': "pk", 'AttributeType': "S"},
                {'AttributeName': "sk", 'AttributeType': "N"}
            ],
            BillingMode='PAY_PER_REQUEST', #  setup on-demand billing
        )

    return {'status', '200'}
