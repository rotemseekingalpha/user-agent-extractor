name := "user-agent-extractor"

version := "0.1"

scalaVersion := "2.11.12"

resolvers ++= Seq(
  "sonatype-releases" at "https://oss.sonatype.org/content/repositories/releases/",
  Resolver.jcenterRepo
)

libraryDependencies ++= Seq(
  "org.apache.spark"          %% "spark-sql"        % "2.4.4",
  "nl.basjes.parse.useragent"  % "yauaa"            % "5.12",
  "org.scalatest"             %% "scalatest"        % "3.1.1" % Test
)

// in case you have a higher version of jackson-databind in your code, add the following:
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7.1"

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case "log4j.propreties" => MergeStrategy.first
  // ----
  // required for spark-sql to read different data types (e.g. parquet/orc/csv...)
  // ----
  case PathList("META-INF", "services", xs @ _*) => MergeStrategy.first
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case n if n.startsWith("reference.conf") => MergeStrategy.concat
  case n if n.endsWith(".conf") => MergeStrategy.concat
  case x => MergeStrategy.first
}

mainClass in assembly := Some("com.seekingalpha.data.spark.UserAgentParserMain")

test in assembly := {}
