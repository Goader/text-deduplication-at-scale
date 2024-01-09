ThisBuild / organization := "ua.nlp.ukrlm"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "text-deduplication-at-scale",
    idePackagePrefix := Some("ua.nlp.ukrlm"),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.2.0",
      "org.apache.spark" %% "spark-sql" % "3.2.0",
    )
  )


