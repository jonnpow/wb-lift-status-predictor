#  dependencies
import requests
import datetime
import time
import boto3

from bs4 import BeautifulSoup


#  lambda function to scrape lift status, weather, and snowfall data from Whistler-Blackcomb
def lambda_handler(event, context):

    today = datetime.datetime.utcnow().date().strftime("%Y-%m-%d")
    report_time = int(time.time())

    #  scrape lift status
    url_epic_lift_status = "https://www.epicmix.com/mobile/mountainstatus_lifts.aspx?resortId=13"

    response = requests.get(url_epic_lift_status)
    soup = BeautifulSoup(response.text, "html.parser")

    #  status
    active_lift = [lift.text.replace(" ", "_") for lift in soup.select('li.lift.' + 'active' + '> h3.name')]
    inactive_lift = [lift.text.replace(" ", "_") for lift in soup.select('li.lift.' + 'inactive' + '> h3.name')]
    hold_lift = [lift.text.replace(" ", "_") for lift in soup.select('li.lift.' + 'hold' + '> h3.name')]

    #  lift metadata
    lift_name = active_lift + inactive_lift + hold_lift
    lift_activity = ['closed'] * len(active_lift) + ['open'] * len(inactive_lift) + ['hold'] * len(hold_lift)
    lift_report_time = [report_time] * len(lift_activity)

    #  store lift status data in dictionary
    lift_status_dict = [{'pk': k, 'sk': v, 'status': w} for k, v, w in zip(lift_name, lift_report_time, lift_activity)]

    #  scrape snowfall data
    url_epic_snow_status = "https://www.epicmix.com/mobile/mountainstatus.aspx?resortId=13"

    response = requests.get(url_epic_snow_status)
    soup = BeautifulSoup(response.text, "html.parser")

    #  24hr, 48hr, and base depth data
    one_day_snow = soup.select('h4.value')[0].text[:-1]
    two_day_snow = soup.select('h4.value')[1].text[:-1]
    base_depth = soup.select('h4.value')[3].text[:-1]

    #  store snowfall data in dictionary
    snow_status_dict = {'pk': 'WB', 'sk': report_time, '24_hour_snowfall': one_day_snow,
                        '48_hour_snowfall': two_day_snow, 'base_depth': base_depth}

    #  scrape weather data from multiple weather stations
    weather_status_dict = []
    stations = ['peak', 'horstman', 'rhl', 'rendezvous', 'crystal', 'pig']

    #  scrape data each station
    for station in stations:

        url_wb_wx = "https://m.whistlerblackcomb.com/m/weather-station-" + station + ".php"

        response = requests.get(url_wb_wx)
        soup = BeautifulSoup(response.text, "html.parser")

        #  weather data
        temperature = soup.select('h3')[0].text.replace("°", "").replace("C", "").split()[0]
        wind_speed = soup.select('h6')[0].text.split()[2]
        wind_gust_speed = soup.select('h6')[0].text.replace(' ', '').split(':')[1].split('km')[0]
        wind_direction = soup.select('h6')[0].text.replace(' ', '').split(':')[-1]

        #  store data in dictionary
        weather_status_dict.append(
            {'pk': station, 'sk': report_time, 'temperature': temperature, 'wind_speed': wind_speed,
             'wind_gust_speed': wind_gust_speed, 'wind_direction': wind_direction})

    #  save lift status data to dynamodb table
    table = boto3.resource('dynamodb', region_name='us-east-2').Table('lift-status_' + today)
    for i in range(0, len(lift_status_dict)):
        table.put_item(Item=lift_status_dict[i])

    #  save snowfall data to dynamodb table
    table = boto3.resource('dynamodb', region_name='us-east-2').Table('snowfall_' + today)
    table.put_item(Item=snow_status_dict)

    #  save weather data to dynamodb table
    table = boto3.resource('dynamodb', region_name='us-east-2').Table('weather_' + today)
    for i in range(0, len(weather_status_dict)):
        table.put_item(Item=weather_status_dict[i])

    return {'status', '200'}