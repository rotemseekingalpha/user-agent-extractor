package com.seekingalpha.data.spark

import java.io.File

import nl.basjes.parse.useragent.UserAgentAnalyzer
import org.apache.commons.io.FileUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

object UserAgentParserMain {

  private final val logger: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      logger.error(s"\nUsage: java ${getClass.getName} [path]")
      System.exit(0)
    }
    val skip = if (args.length > 1 && args(1).equals("-skip")) true else false

    val spark = SparkSession.builder()
      .appName(getClass.getSimpleName)
      .master("local[*]")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.sql.broadcastTimeout", 6600)
      .config("spark.sql.files.ignoreCorruptFiles", "true")
      .config("spark.executor.memory", "16g")
      .config("spark.driver.memory", "16g")
      .getOrCreate

    val path = {
      val arg = args(0)
      val p = if (arg.endsWith("/")) arg else arg.concat(File.separator)
      p.concat("*.parquet")
    }

    val outputDir = args(0) + File.separator + "output" + File.separator
    if (!skip) {
      logger.info(s"Going to read from ${path}")

      val ua: Broadcast[UserAgentAnalyzer] = spark.sparkContext.broadcast(UserAgentAnalyzer.newBuilder().build())
      val udfParseUserAgent: UserDefinedFunction = udf((x: String) => userAgentParser(x, ua))

      val df = spark.read.parquet(path)
        .withColumn(colNameUserAgentJson, udfParseUserAgent(col(colNameUserAgent)))

      val dsUserAgent = df.select(col(colNameUserAgentJson)).as[String](Encoders.STRING)
      val dfUserAgent = spark.read.json(dsUserAgent)

      val userAgentDf = df.withColumn(colNameUserAgentStruct, from_json(col(colNameUserAgentJson), dfUserAgent.schema))
        .drop(colNameUserAgentJson)
        .withColumn(colNameDeviceClass, col(s"$colNameUserAgentStruct.$colNameUserAgentDeviceClass"))
        .withColumn(colNameOsName, col(s"$colNameUserAgentStruct.$colNameUserAgentOsName"))
        .withColumn(colNameOsVersion, col(s"$colNameUserAgentStruct.$colNameUserAgentOsVersion"))
        .withColumn(colNameAgentName, col(s"$colNameUserAgentStruct.$colNameUserAgentAgentName"))
        .withColumn(colNameAgentVersion, col(s"$colNameUserAgentStruct.$colNameUserAgentAgentVersion"))
        .withColumn(colNameDeviceBrand, col(s"$colNameUserAgentStruct.$colNameUserAgentDeviceBrand"))
        .withColumn(colNameDeviceName, col(s"$colNameUserAgentStruct.$colNameUserAgentDeviceName"))
        .drop(colNameUserAgentJson)
        .persist(StorageLevel.MEMORY_AND_DISK_SER)

      FileUtils.deleteQuietly(new File(outputDir))
      userAgentDf.write.parquet(outputDir)
      logger.info(userAgentDf.select(colNameAgentName).distinct().collect().map(_.getString(0)).mkString(","))
    }

    val df = spark.read.parquet(outputDir.concat("*.parquet"))
    logger.info(df.select(colNameAgentName).distinct().collect().map(_.getString(0)).mkString("\n"))

    spark.close()
  }
}
