ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.13"

lazy val root = (project in file("."))
  .settings(
    name := "csv-files-processor",
    idePackagePrefix := Some("pt.claudiopereira.csvfilesprocessor")
  )

val sparkVersion = "3.5.1"

libraryDependencies ++= Seq (
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)
