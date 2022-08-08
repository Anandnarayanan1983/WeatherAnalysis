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
LOCATION '/user/hive/warehouse/gcv360_test.db/stockholm_temperature';

CREATE EXTERNAL TABLE gcv360_test.stockholm_pressure (
year int,
month int,
day int,
morning_observation decimal(7,2),
noon_observation decimal(7,2),
evening_observation decimal(7,2))
PARTITIONED BY (YearID int)
STORED AS PARQUET
LOCATION '/user/hive/warehouse/gcv360_test.db/stockholm_pressure';