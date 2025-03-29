MERGE `hnesman-challenge.hn_etl.hn_locations_weather` final_table
USING (SELECT DISTINCT * FROM `hnesman-challenge.hn_etl.hn_locations_weather_csv`) temp_table
ON
    final_table.city = temp_table.city
    final_table.state = temp_table.state
    final_table.country = temp_table.country
    final_table.lat = temp_table.lat
    final_table.lon = temp_table.lon
    final_table.date = temp_table.date
    
WHEN MATCHED THEN
UPDATE SET 
    final_table.data = temp_table.data
WHEN NOT MATCHED THEN
INSERT
    ROW