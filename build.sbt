ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.11.12"

libraryDependencies ++= Seq (
  "org.apache.spark" %% "spark-catalyst" % "2.4.8",
  "org.apache.spark" %% "spark-core" % "2.4.8",
  "org.apache.spark" %% "spark-hive" % "2.4.8",
  "org.apache.spark" %% "spark-sql" % "2.4.8",
  "com.crealytics" %% "spark-excel" % "0.13.7",
  "com.wolt.osm" %% "spark-osm-datasource" % "0.3.0",
  "com.wolt.osm" % "parallelpbf" % "0.3.1"
)

lazy val root = (project in file("."))
  .settings(
    name := "osm-reader"
  )
