package runners

import com.typesafe.config.ConfigFactory
import model.Strings
import org.apache.spark.sql.SparkSession
import training.pos.PosTrainer

object App {
  def main(args: Array[String]): Unit = {
    val path = ConfigFactory.load().getString(Strings.configPosPipelineModel)

    val sc: SparkSession = SparkSession
      .builder()
      .appName(Strings.sparkParamsAppName)
      .master(Strings.sparkParamsLocal)
      .config(Strings.sparkConigExecuterMemory, Strings.sparkParamsMemory)
      .config(Strings.sparkConfigDriverMemory, Strings.sparkParamsMemory)
      .getOrCreate()

    val trainer = new PosTrainer(sc)
    trainer.startTraining(Some(path))
    trainer.results(None, path, true)
  }
}
