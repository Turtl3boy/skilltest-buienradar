import sqlite3

# Connect to the SQLite database
db_file = "weather_data.sqlite"
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

# Question 5: Which weather station recorded the highest temperature?
cursor.execute("""
SELECT s.stationid, s.stationname, s.regio, MAX(m.temperature) 
FROM measurements m
JOIN stations s ON m.stationid = s.stationid
GROUP BY m.stationid
ORDER BY MAX(m.temperature) DESC
LIMIT 1
""")
highest_temp_station = cursor.fetchone()

if highest_temp_station:
    print(f"Question 5: The weather station with the highest recorded temperature is {highest_temp_station[1]} (ID: {highest_temp_station[0]}) located in {highest_temp_station[2]} with a temperature of {highest_temp_station[3]}°C.")
else:
    print("No data found for the highest recorded temperature.")

# Question 6: What is the average temperature?
cursor.execute("""
SELECT AVG(temperature) FROM measurements
""")
average_temperature = cursor.fetchone()[0]
if average_temperature is not None:
    print(f"Question 6: The average temperature across all stations is {average_temperature:.2f}°C.")
else:
    print("No temperature data available to calculate the average.")

# Question 7: What is the station with the biggest difference between feel temperature and actual temperature?
cursor.execute("""
SELECT s.stationid, s.stationname, s.regio, MAX(ABS(m.feeltemperature - m.temperature)), 
       m.feeltemperature, m.temperature
FROM measurements m
JOIN stations s ON m.stationid = s.stationid
GROUP BY m.stationid
ORDER BY MAX(ABS(m.feeltemperature - m.temperature)) DESC
LIMIT 1
""")
biggest_temp_diff_station = cursor.fetchone()

if biggest_temp_diff_station:
    station_id = biggest_temp_diff_station[0]
    station_name = biggest_temp_diff_station[1]
    station_regio = biggest_temp_diff_station[2]
    temp_diff = biggest_temp_diff_station[3]
    feel_temp = biggest_temp_diff_station[4]
    actual_temp = biggest_temp_diff_station[5]
    
    # Determine whether feel temperature or actual temperature is higher
    if feel_temp > actual_temp:
        temp_comparison = f"the feel temperature is {feel_temp - actual_temp:.2f}°C higher than the actual temperature."
    else:
        temp_comparison = f"the feel temperature is {actual_temp - feel_temp:.2f}°C lower than the actual temperature."
    
    print(f"Question 7: The weather station with the biggest difference between feel temperature and actual temperature is {station_name} (ID: {station_id}) located in {station_regio}.")
    print(f"The biggest difference is {temp_diff:.2f}°C, where {temp_comparison}")
else:
    print("No data found for the biggest temperature difference.")

# Question 8: Which weather station is located in the North Sea?
cursor.execute("""
SELECT stationid, stationname, lat, lon, regio
FROM stations
WHERE regio = 'Noordzee'
""")
north_sea_station = cursor.fetchone()

print(f"Question 8: the weather station located in the North Sea is {north_sea_station[1]}")



# Close the connection
conn.close()
