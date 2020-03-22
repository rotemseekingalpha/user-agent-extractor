package com.seekingalpha.data

import nl.basjes.parse.useragent.{UserAgent, UserAgentAnalyzer}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._

package object spark {

  private final val logger: Logger = LoggerFactory.getLogger(getClass)

  lazy val seekingAlpha = "SeekingAlpha"
  lazy val comSeekingAlpha = s"com.${seekingAlpha.toLowerCase}"

  lazy val colNameUserAgent = "user_agent"
  lazy val colNameUserAgentJson = "user_agent_json"
  lazy val colNameUserAgentStruct = "user_agent_struct"
  lazy val colNameDeviceBrand = "device_brand"
  lazy val colNameDeviceClass = "device_class"
  lazy val colNameOsName = "os_name"
  lazy val colNameOsVersion = "os_version"
  lazy val colNameAgentVersion = "agent_version"
  lazy val colNameDeviceName = "device_name"
  lazy val colNameAgentName = "agent_name"
  lazy val colNameAgentClass = "agent_class"

  lazy val colNameUserAgentDeviceBrand = "DeviceBrand"
  lazy val colNameUserAgentDeviceClass = "DeviceClass"
  lazy val colNameUserAgentOsName = "OperatingSystemName"
  lazy val colNameUserAgentOsNameVersion = "OperatingSystemNameVersion"
  lazy val colNameUserAgentOsVersion = "OperatingSystemVersion"
  lazy val colNameUserAgentAgentName = "AgentName"
  lazy val colNameUserAgentAgentVersion = "AgentVersion"
  lazy val colNameUserAgentDeviceName = "DeviceName"
  lazy val colNameUserAgentAgentClass = "AgentClass"
  lazy val colNameUserAgentAgentNameVersion: String = "AgentNameVersion"

  private lazy val regex = "^sa-.*-wrapper$".r

  private def saParseUserAgent(userAgentString: String): Map[String, String] = {
    try {
      if (userAgentString.contains(seekingAlpha)) {
        val parts = userAgentString.split(";")
        // old iOS App
        val last = parts.last.split(" ")
        val os = last(2).split("/")
        val osName = {
          val v = os(0).toLowerCase
          if (v.equals("ios")) "iOS"
          else if (v.equals("android")) "Android"
          else os(0)
        }
        val m = Map(colNameUserAgentOsName -> osName, colNameUserAgentOsVersion -> os(1))
        // old iOS App
        if (last.length == 7) {
          val agentVersion: String = last(5).split("/").last
          val agentName: String = last(6).split("/").last
          if (agentName.equals("VER")) logger.error(userAgentString)
          m ++ Map(colNameUserAgentAgentName -> agentName, colNameUserAgentAgentVersion -> agentVersion)
        }
        else m
      }
      else if (regex.findFirstIn(userAgentString).isDefined || userAgentString.startsWith("com.seekingalpha.webwrapper")) {
        val parts = userAgentString.split(";")
        val os = parts(2).trim.split(" ")
        if (os.length == 3) Array(os(0), os(2))
        else Array(os(0), os(1))
        Map(colNameUserAgentOsName -> os(0), colNameUserAgentAgentVersion -> os(1))
      }
      else Map()
    } catch {
      case e: ArrayIndexOutOfBoundsException =>
        logger.error(s"error parsing user_agent: $userAgentString", e)
        Map()
    }
  }

  def userAgentParser(str: String, ua: Broadcast[UserAgentAnalyzer]): Option[String] = {
    if (StringUtils.isEmpty(str)) None
    else {
      val agent: UserAgent = ua.value.parse(str)
      val map: Map[String, String] = {
        val m: Map[String, String] =
          agent.getAvailableFieldNames.asScala
            .map(x => {
              val value: String = {
                val v: String = agent.getValue(x)
                if (x.equals(colNameUserAgentOsVersion) || x.equals(colNameUserAgentAgentVersion))
                  v.split(" ").last
                else v
              }
              (x, value)
            })
            .toMap[String, String]
        val os = m(colNameUserAgentOsName)
        if (os.toLowerCase.startsWith("unknown") || os.contains("??")) {
          val sa: Map[String, String] = saParseUserAgent(str)
          mergeMaps(m, sa)
        }
        else m
      }
      val m = map.filter({ case (_, v) => !v.equals("??") || !v.toLowerCase.startsWith("unknown") })
      Some(scala.util.parsing.json.JSONObject(m).toString())
    }
  }

  def mergeMaps[K, V](m1: Map[K, V], m2: Map[K, V]): Map[K, V] = {
    if (m2.isEmpty) m1
    else (m1.keySet ++ m2.keySet).map(k => if (m2.contains(k)) (k, m2(k)) else (k, m1(k))).toMap
  }
}
