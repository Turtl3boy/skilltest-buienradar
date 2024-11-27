import requests
import pandas as pd
import sqlite3
from datetime import datetime

# Buienradar API endpoint
url = "https://data.buienradar.nl/2.0/feed/json"

# Function to fetch data from Buienradar API
def fetch_buienradar_data(api_url):
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from Buienradar API: {e}")
        return None

# Function to extract weather station measurements
def extract_station_measurements(data):
    stations = data.get('actual', {}).get('stationmeasurements', [])
    measurement_data = []

    for station in stations:
        measurement_data.append({
            "measurementid": str(station.get('stationid')) + "_" + station.get('timestamp'),
            "timestamp": station.get('timestamp'),
            "temperature": station.get('temperature'),
            "groundtemperature": station.get('groundtemperature'),
            "feeltemperature": station.get('feeltemperature'),
            "windgusts": station.get('windgusts'),
            "windspeedBft": station.get('windspeedBft'),
            "humidity": station.get('humidity'),
            "precipitation": station.get('precipitation'),
            "sunpower": station.get('sunpower'),
            "stationid": station.get('stationid')
        })

    # Convert to DataFrame
    measurements_df = pd.DataFrame(measurement_data)
    measurements_df['timestamp'] = pd.to_datetime(measurements_df['timestamp'])  # Ensure datetime format
    return measurements_df

# Function to extract weather station metadata
def extract_station_info(data):
    stations = data.get('actual', {}).get('stationmeasurements', [])
    station_data = []

    for station in stations:
        station_data.append({
            "stationid": station.get('stationid'),
            "stationname": station.get('stationname'),
            "lat": station.get('lat'),
            "lon": station.get('lon'),
            "regio": station.get('regio')
        })

    # Convert to DataFrame
    station_info_df = pd.DataFrame(station_data).drop_duplicates()  # Remove duplicates
    return station_info_df

# Function to save DataFrames to SQLite database
def save_to_sqlite(measurements_df, station_info_df, db_name="weather_data.sqlite"):
    # Connect to SQLite database (or create it)
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Create 'stations' table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS stations (
        stationid INTEGER PRIMARY KEY,
        stationname TEXT,
        lat REAL,
        lon REAL,
        regio TEXT
    )
    """)

    # Create 'measurements' table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS measurements (
        measurementid TEXT PRIMARY KEY,
        timestamp DATETIME,
        temperature REAL,
        groundtemperature REAL,
        feeltemperature REAL,
        windgusts REAL,
        windspeedBft INTEGER,
        humidity REAL,
        precipitation REAL,
        sunpower REAL,
        stationid INTEGER,
        FOREIGN KEY (stationid) REFERENCES stations (stationid)
    )
    """)

    # Save station data to 'stations' table
    station_info_df.to_sql('stations', conn, if_exists='replace', index=False)

    # Save measurements data to 'measurements' table
    measurements_df.to_sql('measurements', conn, if_exists='replace', index=False)

    # Commit and close the connection
    conn.commit()
    conn.close()
    print(f"Data successfully saved to {db_name}")

# Main script execution
if __name__ == "__main__":
    print("Fetching data from Buienradar API...")
    data = fetch_buienradar_data(url)

    if data:
        print("Extracting weather station measurements...")
        measurements_df = extract_station_measurements(data)
        print(f"Extracted {len(measurements_df)} measurement records.")

        print("Extracting weather station information...")
        station_info_df = extract_station_info(data)
        print(f"Extracted {len(station_info_df)} station records.")

        # Save datasets to SQLite database
        print("Saving datasets to SQLite database...")
        save_to_sqlite(measurements_df, station_info_df)
    else:
        print("No data fetched from Buienradar API.")
