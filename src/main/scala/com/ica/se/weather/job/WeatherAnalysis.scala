package com.ica.se.weather.job

import com.ica.se.weather.commons.Properties._
import com.typesafe.scalalogging.Logger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.{col, lit, round, trim}
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.io.File
import java.net.URL
import java.time.LocalDateTime
import scala.sys.process._

object WeatherAnalysis {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName(appName).getOrCreate()
    val logger = Logger.apply(this.getClass)

    logger.info("WeatherAnalysis Job Started")

    try {
      //downloading  the  temperature &  air pressure files from the website link & copying it to HDFS
      doDownloadAndCopyHDFS(logger)
      //data cleansing and Processing
      doProcessAndLoadData(spark,logger)
      // repairing the table to include the partitions which are created after the table creation.
      spark.sql(repair_temperature_table)

      spark.sql(repair_pressure_table)

      //creating use case report -  for getting max/min temperature &  pressure at year &  month wise
      val usecase_min_max_weather_report = spark.sql(use_case_sql)

      // writing results into hdfs
      usecase_min_max_weather_report.write
        .mode(SaveMode.Overwrite)
        .parquet(usecase_min_max_temperature_pressure)

      logger.info("WeatherAnalysis Job completed")
    } catch {
      case e: Exception => {

        logger.error(LocalDateTime.now() + e.getMessage())

        throw e
      }
    } finally {

      spark.stop()

    }
  }

  def doDownloadAndCopyHDFS(logger: Logger): Unit = {

    //deleting old input files from local folder
    logger.info("deleting old input files from local folder ")
    Option(
      new File(Temp_local_dir).listFiles
        .filter(_.getName.startsWith(Temp_local_file_name))
        .foreach(_.delete)
    )

    Option(
      new File(Pressure_local_dir).listFiles
        .filter(_.getName.startsWith(Pressure_local_file_name))
        .foreach(_.delete)
    )

    logger.info("downloading all the files from website link to local & copying it to hdfs")

    for (f <- pressure_temp_list) {
      val hadoopConf = new Configuration()
      val hdfs = FileSystem.get(hadoopConf)

      var Url_path = f.split(",")(0)
      var local_path = f.split(",")(1)
      var hdfs_path = f.split(",")(2)
      //downloading the temperature file from website url to local path
      new URL(Url_path) #> new File(local_path) !!

      //copying the file local to hdfs path
      hdfs.copyFromLocalFile(new Path(local_path.toString), new Path(hdfs_path.toString))

    }
  }

  def doProcessAndLoadData(spark: SparkSession,logger: Logger): Unit = {

    logger.info("data cleansing and Processing the source file")

    for (f <- pressure_temp_list) {

      var hdfs_path = f.split(",")(2)

      if (hdfs_path.contains("Temperature")) {

        if (hdfs_path.contains("1756_1858")) {
          // loading the file content into dataframe
          var df_file_content = spark.sqlContext.createDataFrame(spark.sparkContext.textFile(hdfs_path).map { x => getRow_Temperature_1756(x) }, weather_Schema_1756)

          var df_cast_type = df_file_content.select(trim(col("year")).cast("int").alias("year"), trim(col("month")).cast("int").alias("month"), trim(col("day")).cast("int").alias("day"), col("morning_observation").cast("float"), col("noon_observation").cast("float"), lit("0.00").cast("float").alias("evening_observation"), lit("0.00").cast("float").alias("tmax"), lit("0.00").cast("float").alias("tmin"), lit("0.00").cast("float").alias("tmean"), trim(col("year")).cast("int").alias("YearID"))

          var df_final = df_cast_type.na.drop()

          df_final.write.mode(SaveMode.Append).partitionBy("YearID").parquet(hdfs_temperature_path)

        } else if (hdfs_path.contains("_1859_1960")) {

          var df_file_content = spark.sqlContext.createDataFrame(spark.sparkContext.textFile(hdfs_path).map { x => getRow_Temperature_1859(x) }, weather_Schema_1859)

          var df_cast_type = df_file_content.select(trim(col("year")).cast("int").alias("year"), trim(col("month")).cast("int").alias("month"), trim(col("day")).cast("int").alias("day"), col("morning_observation").cast("float"), col("noon_observation").cast("float"), col("evening_observation").cast("float"), col("tmax").cast("float"), col("tmin").cast("float"), lit("0.00").cast("float").alias("tmean"), trim(col("year")).cast("int").alias("YearID"))

          var df_final = df_cast_type.na.drop()

          df_final.write.mode(SaveMode.Append).partitionBy("YearID").parquet(hdfs_temperature_path)

        } else {
          // loading the file content into dataframe
          var df_file_content = spark.sqlContext.createDataFrame(spark.sparkContext.textFile(hdfs_path).map { x => getRow_Temperature(x) }, weather_Schema)

          var df_cast_type = df_file_content.select(trim(col("year")).cast("int").alias("year"), trim(col("month")).cast("int").alias("month"), trim(col("day")).cast("int").alias("day"), col("morning_observation").cast("float"), col("noon_observation").cast("float"), col("evening_observation").cast("float"), col("tmax").cast("float"), col("tmin").cast("float"), col("tmean").cast("float"), trim(col("year")).cast("int").alias("YearID"))
          // removing/cleansing any null entries
          var df_final = df_cast_type.na.drop()

          df_final.write.mode(SaveMode.Append).partitionBy("YearID").parquet(hdfs_temperature_path)
        }

      }
        // processing the air pressure files
      else  {

        if (hdfs_path.contains("_1862_1937")) {

          var df_file_content = spark.sqlContext.createDataFrame(spark.sparkContext.textFile(hdfs_path).map { x => getRow_Pressure_1862(x) }, pressure_Schema)

          // to keep the  units in consistent across years , converting from mmHg to hPa (  1 mmHg = 1.333 hPa )
          var df_cast_type = df_file_content.select(trim(col("year")).cast("int").alias("year"), trim(col("month")).cast("int").alias("month"), trim(col("day")).cast("int").alias("day"), round(col("morning_observation").cast("float").multiply(1.33),2).alias("morning_observation"), round(col("noon_observation").cast("float").multiply(1.3332)).alias("noon_observation"), round(col("evening_observation").cast("float").multiply(1.3332),2).alias("evening_observation"), trim(col("year")).cast("int").alias("YearID"))

          var df_final = df_cast_type.na.drop()

          df_final.write.mode(SaveMode.Append).partitionBy("YearID").parquet(hdfs_airpressure_path)

        } else if (hdfs_path.contains("_1938_1960")) {

          var df_file_content = spark.sqlContext.createDataFrame(spark.sparkContext.textFile(hdfs_path).map { x => getRow_Pressure_1938(x) }, pressure_Schema)

          var df_cast_type = df_file_content.select(trim(col("year")).cast("int").alias("year"), trim(col("month")).cast("int").alias("month"), trim(col("day")).cast("int").alias("day"), col("morning_observation").cast("float").alias("morning_observation"), col("noon_observation").cast("float").alias("noon_observation"), col("evening_observation").cast("float").alias("evening_observation"), trim(col("year")).cast("int").alias("YearID"))

          var df_final = df_cast_type.na.drop()

          df_final.write.mode(SaveMode.Append).partitionBy("YearID").parquet(hdfs_airpressure_path)

        } else {

          var df_file_content = spark.sqlContext.createDataFrame(spark.sparkContext.textFile(hdfs_path).map { x => getRow_Pressure(x) }, pressure_Schema)

          var df_cast_type = df_file_content.select(trim(col("year")).cast("int").alias("year"), trim(col("month")).cast("int").alias("month"), trim(col("day")).cast("int").alias("day"), col("morning_observation").cast("float").alias("morning_observation"), col("noon_observation").cast("float").alias("noon_observation"), col("evening_observation").cast("float").alias("evening_observation"), trim(col("year")).cast("int").alias("YearID"))

          var df_final = df_cast_type.na.drop()

          df_final.write.mode(SaveMode.Append).partitionBy("YearID").parquet(hdfs_airpressure_path)
        }
      }
    }
  }
}



