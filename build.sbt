name := "WeatherAnalysis"

version := "0.1"

scalaVersion := "2.11.8"


resolvers += "Sonatype Nexus Repository Manager" at "https://repository-mgr.ecom.ia.icacorp.net/repository/gcv360/"
credentials += Credentials("Sonatype Nexus Repository Manager", "repository-mgr.ecom.ia.icacorp.net", "gcv360-build", "welcome1")

libraryDependencies += "com.typesafe" % "config" % "1.3.3"
libraryDependencies += "org.scala-lang" % "scala-library" % "2.11.8"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"

libraryDependencies ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-core" % "2.9.0",
  "com.fasterxml.jackson.core" % "jackson-annotations" % "2.9.0",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.0",
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % "2.9.0"
)
libraryDependencies += "org.apache.parquet" % "parquet-common" % "1.8.1"
libraryDependencies += "org.json" % "json" % "20090211"
libraryDependencies += "javax.mail" % "mail" % "1.4.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.6.0"
libraryDependencies += "org.apache.commons" % "commons-io" % "1.3.2"
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.6.0"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.4.0"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"


