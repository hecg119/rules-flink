name := "rules-flink"

version := "0.1"

organization := "org.flink.rules"

ThisBuild / scalaVersion := "2.11.12"

val flinkVersion = "1.6.0"

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided")

lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= flinkDependencies
  )
