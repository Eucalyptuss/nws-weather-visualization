import requests
import folium
import webbrowser
import os

# 댈러스의 위도와 경도
latitude = 32.7767
longitude = -96.7970

# NWS API 엔드포인트
points_url = f"https://api.weather.gov/points/{latitude},{longitude}"

# 위치 정보 가져오기
response = requests.get(points_url)
data = response.json()

# 관측소 URL 추출
observation_stations_url = data['properties']['observationStations']

# 관측소 데이터 가져오기
stations_response = requests.get(observation_stations_url)
stations_data = stations_response.json()
stations = stations_data['observationStations']

# 지도 생성
dallas_map = folium.Map(location=[latitude, longitude], zoom_start=10)

# 각 관측소의 현재 날씨 데이터 가져오기 및 지도에 추가
for station_url in stations:
    current_weather_url = f"{station_url}/observations/latest"
    current_weather_response = requests.get(current_weather_url)
    if current_weather_response.status_code == 200:
        current_weather_data = current_weather_response.json()
        current_observation = current_weather_data['properties']
        station_name = current_weather_data['id'].split('/')[-1]

        temp = current_observation['temperature']['value']
        humidity = current_observation['relativeHumidity']['value']
        wind_speed = current_observation['windSpeed']['value']
        wind_direction = current_observation['windDirection']['value']
        description = current_observation['textDescription']
        coordinates = current_weather_data['geometry']['coordinates']

        # 마커 생성
        marker = folium.Marker(
            location=[coordinates[1], coordinates[0]],
            popup=f"Station: {station_name}<br>"
                  f"Temp: {temp}°C<br>"
                  f"Humidity: {humidity}%<br>"
                  f"Wind Speed: {wind_speed} m/s<br>"
                  f"Wind Direction: {wind_direction}°<br>"
                  f"Condition: {description}",
            tooltip=station_name
        )
        marker.add_to(dallas_map)

# 임시 파일에 지도 저장
map_path = 'weather_map.html'
dallas_map.save(map_path)

# 웹 브라우저에서 지도 열기
webbrowser.open(f"file://{os.path.realpath(map_path)}")

