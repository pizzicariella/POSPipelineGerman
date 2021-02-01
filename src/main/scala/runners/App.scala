package runners

import com.typesafe.config.ConfigFactory
import daos.db.DbDao
import model.Strings
import org.apache.spark.sql.SparkSession
import training.pos.PosTrainer

object App {
  def main(args: Array[String]): Unit = {
    val path = ConfigFactory.load().getString(Strings.configPosPipelineModel)

    val userName = ConfigFactory.load().getString(Strings.dbConfigUser)
    val pw = ConfigFactory.load().getString(Strings.dbConfigPw)
    val serverAddress = ConfigFactory.load().getString(Strings.dbConfigServer)
    val port = ConfigFactory.load().getString(Strings.dbConfigPort)
    val db = ConfigFactory.load().getString(Strings.dbConfigDb)
    val collectionName = ConfigFactory.load().getString(Strings.dbConfigCollection)

    val targetUserName = ConfigFactory.load().getString(Strings.targetDbConfigUser)
    val targetPw = ConfigFactory.load().getString(Strings.targetDbConfigPw)
    val targetServerAddress = ConfigFactory.load().getString(Strings.targetDbConfigServer)
    val targetPort = ConfigFactory.load().getString(Strings.targetDbConfigPort)
    val targetDb = ConfigFactory.load().getString(Strings.targetDbConfigDb)
    val targetCollectionName = ConfigFactory.load().getString(Strings.targetDbConfigCollection)

    val spark: SparkSession = SparkSession
      .builder()
      .appName(Strings.sparkParamsAppName)
      .master(Strings.sparkParamsLocal)
      .config("spark.mongodb.input.uri", "mongodb://"+userName+":"+pw+"@"+serverAddress+":"+port+"/"+db+"."+collectionName)
      .config("spark.mongodb.output.uri", "mongodb://"+targetUserName+":"+targetPw+"@"+targetServerAddress+":"
        +targetPort+"/"+targetDb+"."+targetCollectionName)
      .config(Strings.sparkConigExecuterMemory, Strings.sparkParamsMemory)
      .config(Strings.sparkConfigDriverMemory, Strings.sparkParamsMemory)
      .getOrCreate()

    val dao = new DbDao(spark)

    val trainer = new PosTrainer(spark, Some(200), dao)
    trainer.startTraining(Some(path))
    trainer.results(None, path, true)
  }
}
