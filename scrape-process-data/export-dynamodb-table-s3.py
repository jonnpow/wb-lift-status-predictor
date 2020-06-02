#  dependencies
import boto3
import pandas as pd
import time
import datetime
from io import StringIO
import os

#  start dynamodb client
region = os.environ.get('AWS_DEFAULT_REGION', 'us-east-2')
client = boto3.resource('dynamodb', region_name=region)


# lambda function to export yesterdays data from dynamodb to s3
def lambda_handler(event, context):

    today = datetime.datetime.utcnow().date()
    yesterday = (today - datetime.timedelta(1)).strftime("%Y-%m-%d")

    #  scan dynamodb weather table and save as dataframe
    weather_table = client.Table('weather_' + yesterday)
    data = weather_table.scan()['Items']
    df_wx = pd.DataFrame(data)
    df_wx.columns = ['Time', 'Wind_Dir', 'Station', 'Temp', 'Wind_Spd', 'Wind_Gust']
    df_wx['DateTime_UTC'] = [time.ctime(seconds) for seconds in df['Time']]
    df_wx.set_index('DateTime_UTC', inplace=True)
    df_wx.drop(columns=['Time'], inplace=True)

    #  export weather dataframe to s3
    csv_buffer = StringIO()
    df_wx.to_csv(csv_buffer)
    s3_resource = boto3.resource('s3')
    s3_resource.Object('wb-lift-status-ml', 'weather_' + yesterday + '.csv').put(Body=csv_buffer.getvalue())

    #  scan dynamodb snowfall table and save as dataframe
    snowfall_table = client.Table('snowfall_' + yesterday)
    data = snowfall_table.scan()['Items']
    df_sn = pd.DataFrame(data)
    df_sn.columns = ['Time', 'Station', '48hr_snow', 'base_depth', '24hr_snow']
    df_sn['DateTime_UTC'] = [time.ctime(seconds) for seconds in df['Time']]
    df_sn.set_index('DateTime_UTC', inplace=True)
    df_sn.drop(columns=['Time'], inplace=True)

    #  export snowfall dataframe to s3
    csv_buffer = StringIO()
    df_sn.to_csv(csv_buffer)
    s3_resource = boto3.resource('s3')
    s3_resource.Object('wb-lift-status-ml', 'snowfall_' + yesterday + '.csv').put(Body=csv_buffer.getvalue())

    #  scan dynamodb lift status table and save as dataframe
    lift_status_table = client.Table('lift-status_' + yesterday)
    data = lift_status_table.scan()['Items']
    df_ls = pd.DataFrame(data)
    df_ls.columns = ['Lift', 'Time', 'Status']
    df_ls['DateTime_UTC'] = [time.ctime(seconds) for seconds in df['Time']]
    df_ls.set_index('DateTime_UTC', inplace=True)
    df_ls.drop(columns=['Time'], inplace=True)

    #  export lift status dataframe to s3
    csv_buffer = StringIO()
    df_ls.to_csv(csv_buffer)
    s3_resource = boto3.resource('s3')
    obj = s3_resource.Object('wb-lift-status-ml', 'lift-status_' + yesterday + '.csv')
    obj.put(Body=csv_buffer.getvalue())

    return {'status': '200'}