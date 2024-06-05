import requests

# 댈러스의 위도와 경도
latitude = 32.7767
longitude = -96.7970

# NWS API 엔드포인트
points_url = f"https://api.weather.gov/points/{latitude},{longitude}"

# 위치 정보 가져오기
response = requests.get(points_url)
data = response.json()

# 경고 및 알림 URL 추출
forecast_zone_url = data['properties']['forecastZone']

# 경고 및 알림 데이터 가져오기
alerts_url = f"https://api.weather.gov/alerts/active?zone={forecast_zone_url.split('/')[-1]}"
alerts_response = requests.get(alerts_url)
alerts_data = alerts_response.json()

# 경고 및 알림 정보 출력
alerts = alerts_data['features']

if alerts:
    print("현재 날씨 경고 및 알림:")
    for alert in alerts:
        properties = alert['properties']
        print(f"제목: {properties['headline']}")
        print(f"상태: {properties['event']}")
        print(f"설명: {properties['description']}")
        print(f"지시사항: {properties['instruction']}")
        print("-" * 40)
else:
    print("현재 활성화된 날씨 경고 및 알림이 없습니다.")
