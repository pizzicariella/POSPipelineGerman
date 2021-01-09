package daos

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import daos.memory.InMemoryDao
import model.Strings
import org.apache.spark.sql.SparkSession

//TODO update
class DAOTest extends AnyFunSuite{

  val articleFile = ConfigFactory.load().getString(Strings.configTestFile)

  val spark: SparkSession = SparkSession
    .builder()
    .appName(Strings.sparkParamsAppName)
    .master(Strings.sparkParamsLocal)
    .config(Strings.sparkConigExecuterMemory, Strings.sparkParamsMemory)
    .config(Strings.sparkConfigDriverMemory, Strings.sparkParamsMemory)
    .getOrCreate()

  test("getArticles returns Seq with correct size"){
    val dao = new InMemoryDao(spark, articleFile, "none")
    val result1 = dao.getNewsArticles(Some(20))
    assert(result1.count() === 20)
    val result2 = dao.getNewsArticles(Some(200))
    assert(result2.count() === 100)
  }
}
