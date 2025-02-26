ThisBuild / version := "0.2.5-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

val sparkVersion = "3.4.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.scalatest" %% "scalatest" % "3.2.11",
  "org.mongodb.scala" %% "mongo-scala-driver" % "2.9.0"
)

lazy val root = (project in file("."))
  .settings(
    name := "SparkAdvancedOnline"
  )

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", "services", "org.apache.spark.sql.sources.DataSourceRegister") =>
    MergeStrategy.concat
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}