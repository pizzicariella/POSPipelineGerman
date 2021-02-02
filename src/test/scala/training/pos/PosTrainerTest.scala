package training.pos

import java.io.File
import com.typesafe.config.ConfigFactory
import daos.memory.InMemoryDao
import org.apache.hadoop.mapred.InvalidInputException
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import scala.reflect.io.Directory

class PosTrainerTest extends AnyFunSuite{

  val spark: SparkSession = SparkSession
    .builder()
    .appName("POSPipelineGerman")
    .master("local[*]")
    .config("spark.executor.memory", "12g")
    .config("spark.driver.memory", "12g")
    .getOrCreate()

  val articleFile = ConfigFactory.load().getString("app.inmemoryfile_test")
  val path = "src/test/resources/writeModelTest"
  val destination = "src/test/resources/writeTest"
  val dao = new InMemoryDao(spark, articleFile, destination)
  val posTrainer = new PosTrainer(spark, Some(10), dao)

  test("startTraining with None returns but does not save model"){
    val currentTime = System.currentTimeMillis()
    val model = posTrainer.startTraining(None)
    assert(model.isInstanceOf[PipelineModel])
    val file = new File(path)
    if(file.exists()){
      val modifiedTime = file.lastModified()
      assert(currentTime>modifiedTime)
    } else {
      assertThrows[InvalidInputException]{
        PipelineModel.load(path)
      }
    }
  }

  test("startTraining with Some(path) returns and saves model"){
    val file = new File(path)
    if(file.exists()){
      new Directory(file).deleteRecursively()
    }
    val model = posTrainer.startTraining(Some(path))
    assert(model.isInstanceOf[PipelineModel])
    assert(new File(path).exists())
  }

  test("results with None annotates based on previously saved model"){
    posTrainer.startTraining(Some(path))
    val result = posTrainer.results(None, path, false)
    assert(result.isInstanceOf[DataFrame])
  }

  test("results without save option does not save articles"){
    val file = new File(destination)
    if(file.exists()){
      new Directory(file).deleteRecursively()
    }
    posTrainer.startTraining(Some(path))
    posTrainer.results(None, path, false)
    assert(!file.exists())
  }

  test("results with save option does save articles"){
    val file = new File(destination)
    if(file.exists()){
      new Directory(file).deleteRecursively()
    }
    posTrainer.startTraining(Some(path))
    posTrainer.results(None, path, true)
    assert(file.exists())
  }
}
