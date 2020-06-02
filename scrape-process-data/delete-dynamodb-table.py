#  dependencies
import os
import boto3
import datetime

#  start dynamodb client
region = os.environ.get('AWS_DEFAULT_REGION', 'us-east-2')
client = boto3.resource('dynamodb', region_name=region)

#  name of tables to delete
table_prefixes = ['lift-status_', 'snowfall_', 'weather_']


# lambda function to delete yesterdays table
def lambda_handler(event, context):

    for table_prefix in table_prefixes:

        today = datetime.datetime.utcnow().date()
        yesterday = today - datetime.timedelta(1)

        old_table_name = table_prefix + yesterday.strftime("%Y-%m-%d")

        to_delete = client.Table(old_table_name)
        to_delete.delete()

    return {'status': '200'}