package daos

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import daos.memory.InMemoryDao
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.File

class DAOTest extends AnyFunSuite{

  val articleFile = ConfigFactory.load().getString("app.inmemoryfile_test")

  val spark: SparkSession = SparkSession
    .builder()
    .appName("POSPipelineGerman")
    .master("local[*]")
    .config("spark.executor.memory", "12g")
    .config("spark.driver.memory", "12g")
    .getOrCreate()

  val destinationFile = "src/test/resources/writeTest"

  val dao = new InMemoryDao(spark, articleFile, destinationFile)

  test("getNewsArticles returns DataFrame with correct size"){
    val result1 = dao.getNewsArticles(Some(20))
    assert(result1.isInstanceOf[DataFrame])
    assert(result1.count() === 20)
    val result2 = dao.getNewsArticles(Some(200))
    assert(result2.count() === 100)
  }

  test("getNewsArticles returns DataFrame with correct columns only"){
    val res = dao.getNewsArticles(Some(20))
    assert(res.columns.contains("_id"))
    assert(res.columns.contains("long_url"))
    assert(res.columns.contains("crawl_time"))
    assert(res.columns.contains("title"))
    assert(res.columns.contains("intro"))
    assert(res.columns.contains("text"))
    assert(!res.columns.contains("keywords"))
    assert(!res.columns.contains("published_time"))
    assert(!res.columns.contains("news_site"))
    assert(!res.columns.contains("image_links"))
    assert(!res.columns.contains("description"))
    assert(!res.columns.contains("authors"))
    assert(!res.columns.contains("links"))
  }

  test("writeArticles writes DataFrame to File"){
    val dfToWrite = dao.getNewsArticles(Some(5))
    dao.writeArticles(dfToWrite)
    assert(new File(destinationFile).exists())
  }
}
