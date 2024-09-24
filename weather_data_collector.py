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

# 로그 폴더 설정
log_folder = "weather_log"
if not os.path.exists(log_folder):
    os.makedirs(log_folder)

# 파케이 파일 설정
unique_id = str(datetime.now()).split('.')[0].replace(':', '-')
parquet_filename = os.path.join(log_folder, f"weather_data_{unique_id}.parquet")

# 로그 파일 설정
log_filename = os.path.join(log_folder, f"weather_data_{unique_id}.log")
logging.basicConfig(filename=log_filename, level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# 주요 도시 위도 경도 정보
city_coordinates = {
    "alabama": {"birmingham": (33.5186, -86.8104), "montgomery": (32.3668, -86.3000)},
    "alaska": {"anchorage": (61.2181, -149.9003)},
    "arizona": {"phoenix": (33.4484, -112.0740)},
    "arkansas": {"little_rock": (34.7465, -92.2896)},
    "california": {"los_angeles": (34.0522, -118.2437), "san_francisco": (37.7749, -122.4194)},
    "colorado": {"denver": (39.7392, -104.9903)},
    "connecticut": {"hartford": (41.7637, -72.6851)},
    "delaware": {"wilmington": (39.7391, -75.5398)},
    "florida": {"miami": (25.7617, -80.1918), "orlando": (28.5383, -81.3792)},
    "georgia": {"atlanta": (33.7490, -84.3880)},
    "hawaii": {"honolulu": (21.3069, -157.8583)},
    "idaho": {"boise": (43.6150, -116.2023)},
    "illinois": {"chicago": (41.8781, -87.6298)},
    "indiana": {"indianapolis": (39.7684, -86.1581)},
    "iowa": {"des_moines": (41.5868, -93.6250)},
    "kansas": {"wichita": (37.6872, -97.3301)},
    "kentucky": {"louisville": (38.2527, -85.7585)},
    "louisiana": {"new_orleans": (29.9511, -90.0715)},
    "maine": {"portland": (43.6615, -70.2553)},
    "maryland": {"baltimore": (39.2904, -76.6122)},
    "massachusetts": {"boston": (42.3601, -71.0589)},
    "michigan": {"detroit": (42.3314, -83.0458)},
    "minnesota": {"minneapolis": (44.9778, -93.2650)},
    "mississippi": {"jackson": (32.2988, -90.1848)},
    "missouri": {"kansas_city": (39.0997, -94.5786)},
    "montana": {"billings": (45.7833, -108.5007)},
    "nebraska": {"omaha": (41.2565, -95.9345)},
    "nevada": {"las_vegas": (36.1699, -115.1398)},
    "new_hampshire": {"manchester": (42.9956, -71.4548)},
    "new_jersey": {"newark": (40.7357, -74.1724)},
    "new_mexico": {"albuquerque": (35.0844, -106.6504)},
    "new_york": {"new_york": (40.7128, -74.0060)},
    "north_carolina": {"charlotte": (35.2271, -80.8431)},
    "north_dakota": {"fargo": (46.8772, -96.7898)},
    "ohio": {"columbus": (39.9612, -82.9988)},
    "oklahoma": {"oklahoma_city": (35.4676, -97.5164)},
    "oregon": {"portland": (45.5051, -122.6750)},
    "pennsylvania": {"philadelphia": (39.9526, -75.1652)},
    "rhode_island": {"providence": (41.8239, -71.4128)},
    "south_carolina": {"charleston": (32.7765, -79.9311)},
    "south_dakota": {"sioux_falls": (43.5446, -96.7311)},
    "tennessee": {"nashville": (36.1627, -86.7816)},
    "texas": {"houston": (29.7604, -95.3698), "dallas": (32.7767, -96.7970)},
    "utah": {"salt_lake_city": (40.7608, -111.8910)},
    "vermont": {"burlington": (44.4759, -73.2121)},
    "virginia": {"virginia_beach": (36.8529, -75.9780)},
    "washington": {"seattle": (47.6062, -122.3321)},
    "west_virginia": {"charleston": (38.3498, -81.6326)},
    "wisconsin": {"milwaukee": (43.0389, -87.9065)},
    "wyoming": {"cheyenne": (41.1400, -104.8202)}
}


# 화씨를 섭씨로 변환하는 함수
def fahrenheit_to_celsius(f):
    return (f - 32) * 5.0 / 9.0

# 섭씨를 화씨로 변환하는 함수
def celsius_to_fahrenheit(c):
    return (c * 9.0 / 5.0) + 32

# 체감 온도 계산 함수 (화씨 단위)
def calculate_heat_index(t_f, h):
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
    return t_c - ((100 - h) / 5.0)

# NWS API로부터 실측 데이터를 가져오는 함수
def get_current_weather(latitude, longitude, total_collections, current_collection):
    try:
        # NWS API 엔드포인트
        points_url = f"https://api.weather.gov/points/{latitude},{longitude}"

        # 위치 정보 가져오기
        response = requests.get(points_url, timeout=10)
        response.raise_for_status()  # HTTP 에러 확인
        data = response.json()

        # 관측소 URL 추출
        observation_stations_url = data['properties']['observationStations']

        first_station_printed = False

        # 관측소 데이터 가져오기
        stations_response = requests.get(observation_stations_url, timeout=10)
        stations_response.raise_for_status()  # HTTP 에러 확인
        stations_data = stations_response.json()
        stations = stations_data['observationStations']

        all_station_data = []

        # 각 관측소 데이터 가져오기
        for station_url in stations:
            try:
                # 현재 날씨 데이터 가져오기
                current_weather_url = f"{station_url}/observations/latest"
                current_weather_response = requests.get(current_weather_url, timeout=10)
                current_weather_response.raise_for_status()  # HTTP 에러 확인
                current_weather_data = current_weather_response.json()

                # 관측소 위치 정보 가져오기
                station_info_response = requests.get(station_url, timeout=10)
                station_info_response.raise_for_status()  # HTTP 에러 확인
                station_info_data = station_info_response.json()
                station_name = station_info_data['properties']['name']
                station_location = station_info_data['geometry']['coordinates']

                # 현재 날씨 정보
                current_observation = current_weather_data['properties']
                temperature_value = current_observation['temperature']['value']
                temperature_unit = current_observation['temperature']['unitCode']

                if temperature_unit == 'wmoUnit:degF':
                    temperature_celsius = fahrenheit_to_celsius(temperature_value)
                else:
                    temperature_celsius = temperature_value

                # 필요한 데이터 항목 추가
                humidity = current_observation['relativeHumidity']['value'] if 'relativeHumidity' in current_observation else None
                temperature_fahrenheit = celsius_to_fahrenheit(temperature_celsius) if temperature_unit == 'wmoUnit:degC' else temperature_value
                apparent_temperature_fahrenheit = calculate_heat_index(temperature_fahrenheit, humidity)
                apparent_temperature_celsius = fahrenheit_to_celsius(apparent_temperature_fahrenheit)
                dew_point = calculate_dew_point(temperature_celsius, humidity)

                station_data = {
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'station': station_name,
                    'location': station_location,
                    'temperature': temperature_celsius,
                    'apparent_temperature': apparent_temperature_celsius,
                    'humidity': humidity,
                    'wind_speed': current_observation['windSpeed']['value'] if 'windSpeed' in current_observation else None,
                    'wind_direction': current_observation['windDirection']['value'] if 'windDirection' in current_observation else None,
                    'precipitation': current_observation['precipitationLastHour']['value'] if 'precipitationLastHour' in current_observation else None,
                    'probability_of_precipitation': current_observation['probabilityOfPrecipitation']['value'] if 'probabilityOfPrecipitation' in current_observation else None,
                    'dew_point': dew_point,
                    'pressure': current_observation['barometricPressure']['value'] if 'barometricPressure' in current_observation else None,
                    'uv_index': current_observation['uvIndex']['value'] if 'uvIndex' in current_observation else None,
                    'visibility': current_observation['visibility']['value'] if 'visibility' in current_observation else None,
                    'weather': current_observation['textDescription'],
                    'alerts': ','.join([alert['headline'] for alert in current_observation['alerts']]) if 'alerts' in current_observation else None
                }

                all_station_data.append(station_data)

                if not first_station_printed:
                    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    print(json.dumps(station_data, indent=4))
                    first_station_printed = True

            except Exception as e:
                print(f"Failed to get data for station: {station_url}, error: {e}")
                # print(json.dumps(station_data, indent=4))

        # Parquet 파일에 데이터 저장
        df = pd.DataFrame(all_station_data)
        table = pa.Table.from_pandas(df)

        if os.path.exists(parquet_filename):
            old_data = pq.read_table(parquet_filename)
            table = pa.concat_tables([old_data, table])

        pq.write_table(table, parquet_filename)

        print(f"Collected {current_collection}/{total_collections} data points.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to get data: {e}")


# 스케줄 설정 함수
def set_schedule(interval, unit, latitude, longitude, start_now=True, on_the_hour=False, duration_minutes=None):
    end_time = None
    if duration_minutes:
        end_time = datetime.now() + timedelta(minutes=duration_minutes)

    total_collections = duration_minutes * 60 // interval if unit == "seconds" else duration_minutes // interval
    current_collection = 1

    def job():
        nonlocal current_collection
        get_current_weather(latitude, longitude, total_collections, current_collection)
        current_collection += 1

    def schedule_thread():
        if unit == "hours":
            interval_seconds = interval * 3600
        elif unit == "minutes":
            interval_seconds = interval * 60
        elif unit == "seconds":
            interval_seconds = interval
        else:
            raise ValueError("Invalid unit. Use 'seconds', 'minutes', or 'hours'.")

        if on_the_hour:
            now = datetime.now()
            next_run = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
            delay = (next_run - now).seconds
            print(f"Starting at the next minute: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
            time.sleep(delay)
        elif start_now:
            next_run = datetime.now()
        else:
            next_run = datetime.now() + timedelta(seconds=interval_seconds)

        while not end_time or datetime.now() < end_time:
            now = datetime.now()
            if now >= next_run:
                job()
                next_run = now + timedelta(seconds=interval_seconds)
            time.sleep(1)

        print("Scheduled job has ended.")

    t = threading.Thread(target=schedule_thread)
    t.start()
    return t


def main():
    parser = argparse.ArgumentParser(description="Weather Data Collector")
    parser.add_argument('--interval', type=int, default=5, help="Interval for data collection (default: 5)")
    parser.add_argument('--unit', type=str, default='minutes', choices=['seconds', 'minutes', 'hours'],
                        help="Unit for interval (default: minutes)")
    parser.add_argument('--city', type=str, required=True, help="City name for data collection (format: city,state)")
    parser.add_argument('--duration', type=int, default=60,
                        help="Duration in minutes for data collection (default: 60)")
    parser.add_argument('--start_now', type=bool, default=True, help="Start the collection immediately")

    args = parser.parse_args()

    city_state = args.city.lower().split(',')
    if len(city_state) != 2:
        print(f"Error: Invalid city format. Use 'city,state'.")
        return

    city, state = city_state
    if state not in city_coordinates or city not in city_coordinates[state]:
        print(f"Error: City '{city}' in state '{state}' not found in the city list.")
        return

    latitude, longitude = city_coordinates[state][city]

    print(text2art("Weather Data Collector"))
    print(f"Starting data collection for {city.title()}, {state.upper()} with the following parameters:")
    print(f"Interval: {args.interval} {args.unit}")
    print(f"Latitude: {latitude}")
    print(f"Longitude: {longitude}")
    print(f"Duration: {args.duration} minutes")
    print(f"Start Now: {args.start_now}")

    scheduler_thread = set_schedule(interval=args.interval, unit=args.unit, latitude=latitude, longitude=longitude, start_now=args.start_now, duration_minutes=args.duration)

    scheduler_thread.join()
    print("Main thread has ended.")


if __name__ == "__main__":
    main()
