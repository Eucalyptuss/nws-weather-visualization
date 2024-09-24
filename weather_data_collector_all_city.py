import requests
import threading
import time
from datetime import datetime, timedelta
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import json
import logging
import argparse
from art import text2art
import math

# 로그 폴더 설정
log_folder = "weather_log"
if not os.path.exists(log_folder):
    os.makedirs(log_folder)

# 파케이 파일 설정
unique_id = str(datetime.now()).split('.')[0].replace(':', '-')
parquet_filename = os.path.join(log_folder, f"weather_data_{unique_id}.parquet")
csv_filename = os.path.join(log_folder, f"weather_data_{unique_id}.csv")

# 로그 파일 설정
log_filename = os.path.join(log_folder, f"weather_data_{unique_id}.log")
logging.basicConfig(filename=log_filename, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# JSON 파일을 읽어서 딕셔너리로 변환하는 함수
def load_dict_from_json(filename):
    with open(filename, 'r') as f:
        dictionary = json.load(f)
    print(f"Dictionary loaded from {filename}")
    return dictionary

# 파일에서 딕셔너리 불러오기
city_coordinates = load_dict_from_json('city_coordinates.json')


# 화씨를 섭씨로 변환하는 함수
def fahrenheit_to_celsius(f):
    return (f - 32) * 5.0 / 9.0

# 섭씨를 화씨로 변환하는 함수
def celsius_to_fahrenheit(c):
    return (c * 9.0 / 5.0) + 32

# 체감 온도 계산 함수 (화씨 단위)
def calculate_heat_index(t_f, h):
    # h (humidity) 또는 t_f (temperature)가 None인 경우 NaN 반환
    if t_f is None or h is None:
        return float('nan')

    c1 = -42.379
    c2 = 2.04901523
    c3 = 10.14333127
    c4 = -0.22475541
    c5 = -0.00683783
    c6 = -0.05481717
    c7 = 0.00122874
    c8 = 0.00085282
    c9 = -0.00000199

    hi = (c1 + (c2 * t_f) + (c3 * h) + (c4 * t_f * h) + (c5 * t_f ** 2) +
          (c6 * h ** 2) + (c7 * t_f ** 2 * h) + (c8 * t_f * h ** 2) + (c9 * t_f ** 2 * h ** 2))
    return hi

# 이슬점 계산 함수
def calculate_dew_point(t_c, h):
    # h (humidity) 또는 t_c (temperature)가 None인 경우 NaN 반환
    if t_c is None or h is None:
        return float('nan')
    return t_c - ((100 - h) / 5.0)

# NWS API로부터 실측 데이터를 가져오는 함수
def get_current_weather(latitude, longitude, city, state, current_collection):
    try:
        points_url = f"https://api.weather.gov/points/{latitude},{longitude}"
        response = requests.get(points_url, timeout=10)
        response.raise_for_status()
        data = response.json()
        observation_stations_url = data['properties']['observationStations']
        stations_response = requests.get(observation_stations_url, timeout=10)
        stations_response.raise_for_status()
        stations_data = stations_response.json()
        stations = stations_data['observationStations']

        all_station_data = []
        for station_url in stations:
            try:
                current_weather_url = f"{station_url}/observations/latest"
                current_weather_response = requests.get(current_weather_url, timeout=10)
                current_weather_response.raise_for_status()
                current_weather_data = current_weather_response.json()
                current_observation = current_weather_data['properties']

                station_info_response = requests.get(station_url, timeout=10)
                station_info_response.raise_for_status()
                station_info_data = station_info_response.json()
                station_name = station_info_data['properties']['name']
                station_location = station_info_data['geometry']['coordinates']

                temperature_value = current_observation['temperature']['value']
                temperature_unit = current_observation['temperature']['unitCode']

                # 섭씨와 화씨 온도를 둘 다 저장
                if temperature_unit == 'wmoUnit:degF':
                    temperature_fahrenheit = temperature_value
                    temperature_celsius = fahrenheit_to_celsius(temperature_value)
                else:
                    temperature_celsius = temperature_value
                    temperature_fahrenheit = celsius_to_fahrenheit(temperature_value)

                humidity = current_observation['relativeHumidity']['value'] if 'relativeHumidity' in current_observation else float('nan')
                apparent_temperature_fahrenheit = calculate_heat_index(temperature_fahrenheit, humidity)
                apparent_temperature_celsius = fahrenheit_to_celsius(apparent_temperature_fahrenheit)
                dew_point = calculate_dew_point(temperature_celsius, humidity)
                wind_direction = current_observation['windDirection']['value'] if 'windDirection' in current_observation else float('nan')
                precipitation_value = current_observation.get('precipitationLastHour', {}).get('value', float('nan'))

                if isinstance(station_location, list):
                    station_location = [float(coord) for coord in station_location]  # 각 좌표를 double로 변환

                if precipitation_value is None:
                    precipitation_value = float('nan')  # None을 NaN으로 변환하여 항상 double로 처리

                if isinstance(wind_direction, int):
                    wind_direction = float(wind_direction) # wind_direction을 항상 double로 변환

                station_data = {
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'city': city.title(),
                    'state': state.upper(),
                    'station': station_name,
                    'location': station_location,
                    'temperature_celsius': temperature_celsius,
                    'temperature_fahrenheit': temperature_fahrenheit,
                    'apparent_temperature_celsius': apparent_temperature_celsius,
                    'apparent_temperature_fahrenheit': apparent_temperature_fahrenheit,
                    'humidity': humidity,
                    'wind_speed': current_observation['windSpeed']['value'] if 'windSpeed' in current_observation else float('nan'),
                    'wind_direction': wind_direction,
                    'precipitation': precipitation_value,
                    'dew_point': dew_point,
                    'weather': current_observation['textDescription']
                }

                all_station_data.append(station_data)

            except Exception as e:
                print(f"Failed to get data for station: {station_url}, error: {e}")

        # Parquet 파일에 데이터 저장
        df = pd.DataFrame(all_station_data)
        table = pa.Table.from_pandas(df)

        if os.path.exists(parquet_filename):
            old_data = pq.read_table(parquet_filename)
            table = pa.concat_tables([old_data, table])

        pq.write_table(table, parquet_filename)

        # CSV 파일로 저장
        if os.path.exists(csv_filename):
            df_old = pd.read_csv(csv_filename)
            df_combined = pd.concat([df_old, df], ignore_index=True)
            df_combined.to_csv(csv_filename, index=False)
        else:
            df.to_csv(csv_filename, index=False)

        print(f"Collected {current_collection} data points for {city}, {state}.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to get data for {city}, {state}: {e}")


# 스케줄 설정 함수
def set_schedule(interval, unit, duration_minutes=None):
    end_time = None
    if duration_minutes:
        end_time = datetime.now() + timedelta(minutes=duration_minutes)

    current_collection = 1

    def schedule_thread():
        nonlocal current_collection
        for state, cities in city_coordinates.items():
            for city, (lat, lon) in cities.items():
                get_current_weather(lat, lon, city, state, current_collection)
                current_collection += 1
                if end_time and datetime.now() >= end_time:
                    break
            if end_time and datetime.now() >= end_time:
                break
        print("Scheduled job has ended.")

    t = threading.Thread(target=schedule_thread)
    t.start()
    return t


def main():
    parser = argparse.ArgumentParser(description="Weather Data Collector")
    parser.add_argument('--interval', type=int, default=5, help="Interval for data collection (default: 5)")
    parser.add_argument('--unit', type=str, default='minutes', choices=['seconds', 'minutes', 'hours'],
                        help="Unit for interval (default: minutes)")
    parser.add_argument('--duration', type=int, default=60,
                        help="Duration in minutes for data collection (default: 60)")
    args = parser.parse_args()

    print(text2art("Weather Data Collector"))
    print(f"Starting data collection with the following parameters:")
    print(f"Interval: {args.interval} {args.unit}")
    print(f"Duration: {args.duration} minutes")

    scheduler_thread = set_schedule(interval=args.interval, unit=args.unit, duration_minutes=args.duration)
    scheduler_thread.join()
    print("Main thread has ended.")


if __name__ == "__main__":
    main()
