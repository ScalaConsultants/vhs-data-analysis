import sbt._

lazy val commonSettings = Seq(
  name := "vhs-data-analysis",
  organization := "io.scalac",
  version := "1.0",
  scalaVersion := "2.12.15"
)

lazy val sparkVersion = "3.2.0"
lazy val sparkMongoConnectorVersion = "3.0.1"
lazy val plotlyVersion = "0.8.1"

lazy val commonAppDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.mongodb.spark" %% "mongo-spark-connector" % sparkMongoConnectorVersion
)

lazy val enrichVHSDataDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
)

lazy val analyzeVHSDataDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "org.plotly-scala" %% "plotly-render" % plotlyVersion
)

lazy val commonSource = (project in file("common-source"))
  .settings(commonSettings: _*)
  .settings(name := "common-source")
  .settings(libraryDependencies ++= commonAppDependencies)

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
  .dependsOn(commonSource)

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
    assembly / mainClass := Some("Main")
  )
  .dependsOn(commonSource)