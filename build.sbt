ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "text-deduplication-at-scale",
    idePackagePrefix := Some("ua.nlp.ukrlm")
  )
