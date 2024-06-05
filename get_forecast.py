import requests
import pandas as pd
import matplotlib.pyplot as plt

# 댈러스의 위도와 경도
latitude = 32.7767
longitude = -96.7970

# NWS API 엔드포인트
points_url = f"https://api.weather.gov/points/{latitude},{longitude}"
response = requests.get(points_url)
data = response.json()

# 예보 URL 추출
forecast_url = data['properties']['forecast']

# 예보 데이터 가져오기
forecast_response = requests.get(forecast_url)
forecast_data = forecast_response.json()

# 날짜별 최고/최저 온도 데이터 추출
periods = forecast_data['properties']['periods']
dates = [period['startTime'][:10] for period in periods if period['isDaytime']]
highs_f = [period['temperature'] for period in periods if period['isDaytime']]
lows_f = [period['temperature'] for period in periods if not period['isDaytime']]

# 화씨에서 섭씨로 변환
highs_c = [(temp - 32) * 5/9 for temp in highs_f]
lows_c = [(temp - 32) * 5/9 for temp in lows_f]

# 데이터프레임 생성
df = pd.DataFrame({
    'Date': dates[:len(lows_c)],
    'High (°C)': highs_c[:len(lows_c)],
    'Low (°C)': lows_c
})

# 데이터 시각화
plt.figure(figsize=(10, 5))
plt.plot(df['Date'], df['High (°C)'], label='High Temp (°C)', marker='o')
plt.plot(df['Date'], df['Low (°C)'], label='Low Temp (°C)', marker='o')
plt.fill_between(df['Date'], df['High (°C)'], df['Low (°C)'], color='grey', alpha=0.2)
plt.xlabel('Date')
plt.ylabel('Temperature (°C)')
plt.title('7-Day Temperature Forecast for Dallas, TX')
plt.xticks(rotation=45)
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()