package com.seekingalpha.data.spark

import nl.basjes.parse.useragent.UserAgentAnalyzer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.scalatest.funsuite.AnyFunSuite

class UserAgentParserTest extends AnyFunSuite {
  private final lazy val userAgentJson = "[{\"user_agent\":\"Mozilla/5.0 (iPhone SeekingAlphaiPhoneApp; U; iPhone8,2; en-US) IOS/12.3.1 USERID/50256750 DEVICETOKEN/0d3e0458e98430fe97ec19e87cbb9e53e58e4ba4 VER/3.11.4 KIND/portfolio\"},{\"user_agent\":\"sa-ios-samw-wrapper/5.0.1 (iPad; iPad4,7; iOS/12.4.5)\"},{\"user_agent\":\"com.seekingalpha.webwrapper/4.29.3(331) (Linux; U; Android Mobile 7.1.1; es-es; SM-T550 Build/NMF26X; samsung) 768X1024 samsung SM-T550 SAUSER-50140609\"},{\"user_agent\":\"Mozilla/5.0 (iPad; CPU OS 12_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML,like Gecko) Version/12.0 Mobile/15E148 Safari/604.1\"},{\"user_agent\": \"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36\"}]"
  private final lazy val dfSchema: StructType = StructType(
    Seq(
      StructField(colNameUserAgent, StringType, nullable = true)
    )
  )

  private final lazy val userAgentSchema: StructType = StructType(
    Seq(
      StructField(colNameUserAgentOsName, StringType, nullable = true),
      StructField("OperatingSystemNameVersionMajor", StringType, nullable = true),
      StructField(colNameUserAgentDeviceBrand, StringType, nullable = true),
      StructField("LayoutEngineNameVersionMajor", StringType, nullable = true),
      StructField(colNameUserAgentOsNameVersion, StringType, nullable = true),
      StructField("LayoutEngineNameVersion", StringType, nullable = true),
      StructField(colNameUserAgentDeviceClass, StringType, nullable = true),
      StructField("LayoutEngineVersionMajor", StringType, nullable = true),
      StructField(colNameUserAgentAgentNameVersion, StringType, nullable = true),
      StructField("AgentVersion", StringType, nullable = true),
      StructField("OperatingSystemVersionMajor", StringType, nullable = true),
      StructField(colNameUserAgentOsVersion, StringType, nullable = true),
      StructField("AgentLanguage", StringType, nullable = true),
      StructField("AgentVersionMajor", StringType, nullable = true),
      StructField("LayoutEngineVersion", StringType, nullable = true),
      StructField(colNameUserAgentAgentName, StringType, nullable = true),
      StructField("LayoutEngineClass", StringType, nullable = true),
      StructField("AgentLanguageCode", StringType, nullable = true),
      StructField("LayoutEngineName", StringType, nullable = true),
      StructField(colNameUserAgentDeviceName, StringType, nullable = true),
      StructField("AgentNameVersionMajor", StringType, nullable = true),
      StructField(colNameUserAgentAgentClass, StringType, nullable = true),
      StructField("AgentSecurity", StringType, nullable = true),
      StructField("OperatingSystemClass", StringType, nullable = true)
    )
  )
  test("UserAgentParser should parse SeekingAlpha user agents") {
    val data = Array(userAgentJson)

    val spark = SparkSession.builder().master("local[*]").appName(getClass.getName).getOrCreate()

    import spark.implicits._

    val ds: Dataset[String] = spark.sparkContext.parallelize(data).toDF.as[String]
    val df = spark.read.schema(dfSchema).json(ds)

    val ua: Broadcast[UserAgentAnalyzer] = spark.sparkContext.broadcast[UserAgentAnalyzer](UserAgentAnalyzer.newBuilder().build())
    val uaParserUdf: UserDefinedFunction = udf((x: String) => userAgentParser(x, ua))

    val jsonDs = df.withColumn(colNameUserAgentJson, uaParserUdf(col(colNameUserAgent))).select(colNameUserAgentJson).as[String]
    val json = spark.read.schema(userAgentSchema).json(jsonDs)

    json.select(colNameUserAgentOsName, colNameUserAgentOsVersion, colNameUserAgentAgentName, colNameUserAgentAgentVersion, colNameUserAgentDeviceBrand, colNameUserAgentDeviceName).show

    def assertColEquals(filteredCol: String, filteredBy: String, selectedCol: String, expected: String): Unit = {
      val os = json.filter(col(filteredCol).startsWith(lit(filteredBy))).select(selectedCol).collect().map(_.getString(0)).head
      assert(os.equals(expected), s"os [value] $os is not equal to [$expected]")
    }

    assertColEquals(colNameUserAgentAgentName, "sa-ios-samw-wrapper", colNameUserAgentOsName, "iOS")
    assertColEquals(colNameUserAgentAgentName, "sa-ios-samw-wrapper", colNameUserAgentDeviceBrand, "Apple")
    assertColEquals(colNameUserAgentAgentName, "com.seekingalpha.webwrapper", colNameUserAgentOsName, "Android")
    assertColEquals(colNameUserAgentAgentName, "com.seekingalpha.webwrapper", colNameUserAgentDeviceBrand, "Samsung")
    assertColEquals(colNameUserAgentAgentName, "portfolio", colNameUserAgentOsName, "iOS")
    assertColEquals(colNameUserAgentAgentName, "portfolio", colNameUserAgentAgentVersion, "3.11.4")

    spark.close()
  }
}
