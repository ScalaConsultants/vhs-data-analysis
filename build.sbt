import sbt._

lazy val commonSettings = Seq(
  name := "vhs-data-analysis",
  organization := "io.scalac",
  version := "1.0",
  scalaVersion := "2.12.15"
)

lazy val sparkVersion = "3.2.0"
lazy val sparkMongoConnectorVersion = "3.0.1"

lazy val enrichVHSDataDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.mongodb.spark" %% "mongo-spark-connector" % sparkMongoConnectorVersion
)

lazy val analyzeVHSDataDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
)

lazy val enrichVHSData = (project in file("enrich-vhs-data"))
  .settings(commonSettings: _*)
  .settings(name := "enrich-vhs-data")
  .settings(libraryDependencies ++= enrichVHSDataDependencies)
  .settings(
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _ @ _*) => MergeStrategy.discard
      case _ => MergeStrategy.first
    },
    assembly / assemblyJarName := "enrich-vhs-data.jar",
    assembly / mainClass := Some("VHSDataEnricher")
  )

lazy val analyzeVHSData = (project in file("analyze-vhs-data"))
  .settings(commonSettings: _*)
  .settings(name := "analyze-vhs-data")
  .settings(libraryDependencies ++= analyzeVHSDataDependencies)
  .settings(
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _ @ _*) => MergeStrategy.discard
      case _ => MergeStrategy.first
    },
    assembly / assemblyJarName := "analyze-vhs-data.jar",
    assembly / mainClass := Some("VHSDataAnalyzer")
  )