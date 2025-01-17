name := "POSPipelineGerman"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++=Seq(
  "org.apache.spark" %% "spark-core" % "2.4.7" % "provided",
  "com.johnsnowlabs.nlp" %% "spark-nlp" % "2.6.2" % "provided",
  "org.apache.spark" %% "spark-mllib" % "2.4.7" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.4.7" % "provided",
  "org.mongodb.spark" %% "mongo-spark-connector" % "2.4.2",
  "com.typesafe" % "config" % "1.4.1",
  "org.scalactic" %% "scalactic" % "3.2.2",
  "org.scalatest" %% "scalatest" % "3.2.2" % "test",
)

Test / parallelExecution := false

test in assembly := {}

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "application.conf"            => MergeStrategy.concat
  case "reference.conf"              => MergeStrategy.concat
  case x => MergeStrategy.first
}


