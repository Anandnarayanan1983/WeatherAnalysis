CREATE EXTERNAL TABLE gcv360_test.stockholm_temperature (
year int,
month int,
day int,
morning_observation decimal(7,2),
noon_observation decimal(7,2),
evening_observation decimal(7,2),
tmax decimal(7,2),
tmin decimal(7,2),
tmean decimal(7,2))
PARTITIONED BY (YearID int)
STORED AS PARQUET
LOCATION '/user/hive/external/gcv360_test.db/stockholm_temperature';

CREATE EXTERNAL TABLE gcv360_test.stockholm_pressure (
year int,
month int,
day int,
morning_observation decimal(7,2),
noon_observation decimal(7,2),
evening_observation decimal(7,2))
PARTITIONED BY (YearID int)
STORED AS PARQUET
LOCATION '/user/hive/external/gcv360_test.db/stockholm_pressure';

CREATE EXTERNAL TABLE gcv360_test.weather_report (
yearid int,
month int,
morning_minimum_temp decimal(7,2),
morning_maximum_temp decimal(7,2),
noon_minimum_temp decimal(7,2),
noon_maximum_temp decimal(7,2),
evening_minimum_temp decimal(7,2),
evening_maximum_temp decimal(7,2),
morning_minimum_pressure decimal(7,2),
morning_maximum_pressure decimal(7,2),
noon_minimum_pressure decimal(7,2),
noon_maximum_pressure decimal(7,2),
evening_minimum_pressure decimal(7,2),
evening_maximum_pressure decimal(7,2))
STORED AS PARQUET
LOCATION '/user/hive/external/gcv360_test.db/weather_report';
