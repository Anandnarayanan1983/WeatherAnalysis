package com.ica.se.weather.commons

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
object Properties {
  lazy val conf = ConfigFactory.load("application")
  lazy val appName = conf.getString("appName")
  lazy val Url_file_path = conf.getString("Url_file_path").toString
  lazy val Temp_local_file_name = conf.getString("Temp_local_file_name").toString
  lazy val Temp_local_dir = conf.getString("Temp_local_dir").toString
  lazy val Pressure_local_file_name = conf.getString("Pressure_local_file_name").toString
  lazy val Pressure_local_dir = conf.getString("Pressure_local_dir").toString
  lazy val hdfs_temperature_path = conf.getString("hdfs_temperature_path").toString
  lazy val hdfs_airpressure_path = conf.getString("hdfs_airpressure_path").toString
  lazy val usecase_min_max_temperature_pressure = conf.getString("usecase_min_max_temperature_pressure").toString
  lazy val use_case_sql = conf.getString("use_case_sql")
  lazy val repair_temperature_table =  conf.getString("repair_temperature_table")
  lazy val repair_pressure_table =  conf.getString("repair_pressure_table")




  lazy val weather_Schema = new StructType()
    .add("year", StringType, nullable = true)
    .add("month", StringType, nullable = true)
    .add("day", StringType, nullable = true)
    .add("morning_observation", StringType, nullable = true)
    .add("noon_observation", StringType, nullable = true)
    .add("evening_observation", StringType, nullable = true)
    .add("tmax", StringType, nullable = true)
    .add("tmin", StringType, nullable = true)
    .add("tmean", StringType, nullable = true)

  lazy val weather_Schema_1756 = new StructType()
    .add("year", StringType, nullable = true)
    .add("month", StringType, nullable = true)
    .add("day", StringType, nullable = true)
    .add("morning_observation", StringType, nullable = true)
    .add("noon_observation", StringType, nullable = true)
    .add("evening_observation", StringType, nullable = true)


  lazy val weather_Schema_1859 = new StructType()
    .add("year", StringType, nullable = true)
    .add("month", StringType, nullable = true)
    .add("day", StringType, nullable = true)
    .add("morning_observation", StringType, nullable = true)
    .add("noon_observation", StringType, nullable = true)
    .add("evening_observation", StringType, nullable = true)
    .add("tmax", StringType, nullable = true)
    .add("tmin", StringType, nullable = true)


  lazy val pressure_Schema = new StructType()
    .add("year", StringType, nullable = true)
    .add("month", StringType, nullable = true)
    .add("day", StringType, nullable = true)
    .add("morning_observation", StringType, nullable = true)
    .add("noon_observation", StringType, nullable = true)
    .add("evening_observation", StringType, nullable = true)

  def getRow_Temperature(x : String) : Row={
    val columnArray = new Array[String](9)
    columnArray(0)=x.substring(0,4)
    columnArray(1)=x.substring(5,7)
    columnArray(2)=x.substring(8,10)
    columnArray(3)=x.substring(12,17)
    columnArray(4)=x.substring(18,23)
    columnArray(5)=x.substring(24,29)
    columnArray(6)=x.substring(30,34)
    columnArray(7)=x.substring(35,41)
    columnArray(8)=x.substring(42,46)
    Row.fromSeq(columnArray)
  }

  def getRow_Temperature_1756(x : String) : Row={
    val columnArray = new Array[String](6)
    columnArray(0)=x.substring(0,4)
    columnArray(1)=x.substring(5,7)
    columnArray(2)=x.substring(8,10)
    columnArray(3)=x.substring(12,17)
    columnArray(4)=x.substring(18,23)
    columnArray(5)=x.substring(24,29)
    Row.fromSeq(columnArray)
  }

  def getRow_Temperature_1859(x : String) : Row={
    val columnArray = new Array[String](8)
    columnArray(0)=x.substring(0,4)
    columnArray(1)=x.substring(5,7)
    columnArray(2)=x.substring(8,10)
    columnArray(3)=x.substring(12,17)
    columnArray(4)=x.substring(18,23)
    columnArray(5)=x.substring(24,29)
    columnArray(6)=x.substring(30,34)
    columnArray(7)=x.substring(35,40)
    Row.fromSeq(columnArray)
  }

  def getRow_Pressure_1862(x : String) : Row={
    val columnArray = new Array[String](6)
    columnArray(0)=x.substring(0,5)
    columnArray(1)=x.substring(6,9)
    columnArray(2)=x.substring(10,13)
    columnArray(3)=x.substring(14,20)
    columnArray(4)=x.substring(21,27)
    columnArray(5)=x.substring(28,34)
    Row.fromSeq(columnArray)
  }


  def getRow_Pressure_1938(x : String) : Row={
    val columnArray = new Array[String](6)
    columnArray(0)=x.substring(0,5)
    columnArray(1)=x.substring(6,9)
    columnArray(2)=x.substring(10,13)
    columnArray(3)=x.substring(14,21)
    columnArray(4)=x.substring(22,29)
    columnArray(5)=x.substring(30,37)
    Row.fromSeq(columnArray)
  }

  def getRow_Pressure(x : String) : Row={
    val columnArray = new Array[String](6)
    columnArray(0)=x.substring(0,4)
    columnArray(1)=x.substring(5,7)
    columnArray(2)=x.substring(8,10)
    columnArray(3)=x.substring(11,17)
    columnArray(4)=x.substring(18,24)
    columnArray(5)=x.substring(25,31)
    Row.fromSeq(columnArray)
  }

  lazy val pressure_temp_list = List("https://bolin.su.se/data/stockholm-thematic/files/stockholm-historical-weather-observations-2017/temperature/daily/raw/stockholm_daily_temp_obs_1756_1858_t1t2t3.txt,/data/srvacgcv/weathercheck/Temperature/Temperature_1756_1858.csv,/user/hive/warehouse/gcv360_test.db/weather_analysis/Temperature/Temperature_1756_1858.csv" ,"https://bolin.su.se/data/stockholm-thematic/files/stockholm-historical-weather-observations-2017/temperature/daily/raw/stockholm_daily_temp_obs_1859_1960_t1t2t3txtn.txt,/data/srvacgcv/weathercheck/Temperature/Temperature_1859_1960.csv,/user/hive/warehouse/gcv360_test.db/weather_analysis/Temperature/Temperature_1859_1960.csv" ,"https://bolin.su.se/data/stockholm-thematic/files/stockholm-historical-weather-observations-2017/temperature/daily/raw/stockholm_daily_temp_obs_1961_2012_t1t2t3txtntm.txt,/data/srvacgcv/weathercheck/Temperature/Temperature_1961_2012.csv,/user/hive/warehouse/gcv360_test.db/weather_analysis/Temperature/Temperature_1961_2012.csv" ,"https://bolin.su.se/data/stockholm-thematic/files/stockholm-historical-weather-observations-2017/temperature/daily/raw/stockholmA_daily_temp_obs_2013_2017_t1t2t3txtntm.txt,/data/srvacgcv/weathercheck/Temperature/Temperature_2013_2017.csv,/user/hive/warehouse/gcv360_test.db/weather_analysis/Temperature/Temperature_2013_2017.csv"  ,"https://bolin.su.se/data/stockholm-thematic/files/stockholm-historical-weather-observations-2017/air_pressure/raw/stockholm_barometer_1862_1937.txt,/data/srvacgcv/weathercheck/Pressure/Pressure_1862_1937.csv,/user/hive/warehouse/gcv360_test.db/weather_analysis/Pressure/Pressure_1862_1937.csv" ,"https://bolin.su.se/data/stockholm-thematic/files/stockholm-historical-weather-observations-2017/air_pressure/raw/stockholm_barometer_1938_1960.txt,/data/srvacgcv/weathercheck/Pressure/Pressure_1938_1960.csv,/user/hive/warehouse/gcv360_test.db/weather_analysis/Pressure/Pressure_1938_1960.csv" ,"https://bolin.su.se/data/stockholm-thematic/files/stockholm-historical-weather-observations-2017/air_pressure/raw/stockholm_barometer_1961_2012.txt,/data/srvacgcv/weathercheck/Pressure/Pressure_1961_2012.csv,/user/hive/warehouse/gcv360_test.db/weather_analysis/Pressure/Pressure_1961_2012.csv" ,"https://bolin.su.se/data/stockholm-thematic/files/stockholm-historical-weather-observations-2017/air_pressure/raw/stockholmA_barometer_2013_2017.txt,/data/srvacgcv/weathercheck/Pressure/Pressure_2013_2017.csv,/user/hive/warehouse/gcv360_test.db/weather_analysis/Pressure/Pressure_2013_2017.csv")

}
