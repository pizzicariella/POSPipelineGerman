name := "POSPipelineGerman"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++=Seq(
  "org.apache.spark" %% "spark-core" % "2.4.7",
  "com.johnsnowlabs.nlp" %% "spark-nlp" % "2.6.2",
  "com.johnsnowlabs.nlp" %% "spark-nlp-gpu" % "2.6.2",
  "org.apache.spark" %% "spark-mllib" % "2.4.7",
  "org.apache.spark" %% "spark-sql" % "2.4.7",

  "org.mongodb.spark" %% "mongo-spark-connector" % "2.4.2",

  "com.typesafe" % "config" % "1.4.1",

  "org.scalactic" %% "scalactic" % "3.2.2",
  "org.scalatest" %% "scalatest" % "3.2.2" % "test",
)

Test / parallelExecution := false

