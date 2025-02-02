--create keyspace
CREATE KEYSPACE IF NOT EXISTS traffic_weather_keyspace
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};


--create stream_traffic_table
CREATE TABLE IF NOT EXISTS traffic_weather_keyspace.stream_traffic_table(
    street TEXT,
    bicycle INT,
    car INT,
    motorcycle INT, 
    bus INT,
    truck INT,
    traffic_light INT,
    stop_sign INT,
    execution_time TIMESTAMP,
    traffic_density TEXT,
    PRIMARY KEY (street, execution_time)
);


--create stream_weather_table
CREATE TABLE IF NOT EXISTS traffic_weather_keyspace.stream_weather_table(
    street TEXT,
    weather_main TEXT,
    weather_description TEXT,
    temperature FLOAT,
    feels_like_temperature FLOAT,
    temp_min FLOAT,
    temp_max FLOAT,
    pressure FLOAT,
    humidity FLOAT,
    wind_speed FLOAT,
    wind_deg FLOAT,
    execution_time TIMESTAMP,
    humidity_level TEXT,
    thermal_comfort TEXT,
    wind_direction TEXT,
    humidex FLOAT,
    heat_index FLOAT,
    PRIMARY KEY (street, execution_time)
);