package training.pos

import java.io.File

import com.typesafe.config.ConfigFactory
import daos.memory.InMemoryDao
import model.{AnnotatedArticle, Strings}
import org.apache.hadoop.mapred.InvalidInputException
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import utils.Conversion
import scala.reflect.io.Directory

//TODO update
//This tests requires correctly configured database that contains documents with NewsArticle columns.
class PosTrainerTest extends AnyFunSuite{

  val spark: SparkSession = SparkSession
    .builder()
    .appName(Strings.sparkParamsAppName)
    .master(Strings.sparkParamsLocal)
    .config(Strings.sparkConigExecuterMemory, Strings.sparkParamsMemory)
    .config(Strings.sparkConfigDriverMemory, Strings.sparkParamsMemory)
    .getOrCreate()

  val articleFile = ConfigFactory.load().getString(Strings.configTestFile)
  val path = "src/test/resources/posPipelineModel"
  val dao = new InMemoryDao(spark, articleFile, "none")
  val replacements = Seq((Strings.replacePatternSpecialWhitespaces, Strings.replacementWhitespaces),
    (Strings.replacePatternMissingWhitespaces, Strings.replacementMissingWhitespaces))
  val newsArticles = Conversion.prepareArticlesForPipeline(dao.getNewsArticles(Some(20)), replacements)

  val posTrainer = new PosTrainer(spark, Some(20))

  test("startTraining with None returns but does not save model"){
    val model = posTrainer.startTraining(None)
    assert(model.isInstanceOf[PipelineModel])
    assertThrows[InvalidInputException]{
      PipelineModel.load(path)
    }
  }

  test("results with None annotates based on previously saved model"){
    posTrainer.startTraining(Some(path))
    val result = posTrainer.results(None, path, false)
    assert(result.isInstanceOf[Seq[AnnotatedArticle]])
    val directory = new Directory(new File(path))
    directory.deleteRecursively()
  }

}
