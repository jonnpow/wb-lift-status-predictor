#  dependencies
import boto3
import os
import pandas as pd

#  start s3 client
client = boto3.client('s3', region_name='us-east-2')
response = client.list_objects_v2(Bucket='wb-lift-status-ml')

#  get object filename s3 bucket
filename = []
for obj in response['Contents']:
    filename.append(obj['Key'])

#  download object files to local machine
for file in filename:
    client.download_file('wb-lift-status-ml', file, 'data/{}'.format(file))

#  seperate files into lift status, weather, and snowfall
dir_contents = os.listdir('data')

lift_status_files = [file for file in dir_contents if file.startswith('lift-status')]
snowfall_files = [file for file in dir_contents if file.startswith('snowfall')]
wx_files = [file for file in dir_contents if file.startswith('weather')]


#  function to combine multiple CSV files into one dataframe and process data
def combine_process_csv(filenames, column_names):

    #  read csv data to dataframe
    df = pd.concat((pd.read_csv('data/' + file, parse_dates=['DateTime_UTC']) for file in filenames))

    #  process data
    df.sort_values(by='DateTime_UTC', inplace=True)
    df.columns = column_names
    df = df.loc[df['date/time'] < '2020-03-14']  #  get rid of datapoints after resort closed

    return df


#  process lift status data and save to csv
df = combine_process_csv(lift_status_files, ['date/time', 'lift-name', 'status'])
df.to_csv('data/PROCESSED-lift-status.csv', index=None)

#  process weather data and save to csv
df = combine_process_csv(wx_files, ['date/time', 'wind_dir', 'station', 'temperature', 'wind_spd', 'wind_gst'])
df.to_csv('data/PROCESSED-weather.csv', index=None)

#  process snowfall data and save to csv
df = combine_process_csv(snowfall_files, ['date/time', '48hr_snowfall', 'base_depth', '24hr_snowfall'])
df[['48hr_snow', 'base_depth', '24hr_snow']] *= 2.54  #  inches to cm
df.to_csv('data/PROCESSED-snowfall.csv', index=None)
