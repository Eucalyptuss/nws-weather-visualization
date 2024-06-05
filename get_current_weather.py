import requests

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

# 첫 번째 관측소 데이터 가져오기
station_url = stations[0]

# 현재 날씨 데이터 가져오기
current_weather_url = f"{station_url}/observations/latest"
current_weather_response = requests.get(current_weather_url)
current_weather_data = current_weather_response.json()

# 현재 날씨 정보 출력
current_observation = current_weather_data['properties']

print("현재 날씨:")
print(f"온도: {current_observation['temperature']['value']}°C")
print(f"습도: {current_observation['relativeHumidity']['value']}%")
print(f"바람 속도: {current_observation['windSpeed']['value']} m/s")
print(f"풍향: {current_observation['windDirection']['value']}°")
print(f"날씨 상태: {current_observation['textDescription']}")
