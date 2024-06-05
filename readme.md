# NWS API Weather Data Visualization

This repository contains Python scripts to fetch and visualize weather data using the National Weather Service (NWS) API. The scripts can retrieve weather forecasts, current weather conditions, and weather alerts for a given location (Dallas, TX in this example). The data is visualized using various tools, including matplotlib and folium.

## Prerequisites

- Python 3.x
- Required Python libraries:
  - `requests`
  - `pandas`
  - `matplotlib`
  - `folium`

You can install the required libraries using pip:

```bash
pip install requests pandas matplotlib folium
```


## Usage

1. Clone the repository:
```bash
git clone https://github.com/yourusername/nws-weather-visualization.git
cd nws-weather-visualization
```


2. Run the desired script:
   - get_forecast.py   
     - Fetches and visualizes a 7-day weather forecast for Dallas, TX, converting temperatures from Fahrenheit to Celsius.
        ```bash
        python get_forecast.py
        ```
   - get_current_weather.py
     - Fetches and prints the current weather conditions for Dallas, TX from the nearest weather station.
      ```bash
      python get_current_weather.py
      ```
   - get_weather_alerts.py
     - Fetches and prints any active weather alerts for Dallas, TX.
      ```bash
      python get_weather_alerts.py
      ```
   - visualize_weather_stations.py
     - Fetches the current weather conditions from multiple weather stations in the Dallas, TX area and visualizes them on a map using folium. The map is opened directly in the web browser. 
      ```bash
      python visualize_weather_stations.py
      ```



## Acknowledgements
[National Weather Service API](https://www.weather.gov/documentation/services-web-api)


## License
This project is distributed under the MIT License.