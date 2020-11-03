package data

import com.typesafe.config.ConfigFactory
import data.memory.InMemoryDao
import org.scalatest.funsuite.AnyFunSuite

class DAOTest extends AnyFunSuite{

  val articleFile = ConfigFactory.load().getString("app.inmemoryfile_test")
  val columns = Array("title", "intro", "text")

  test("getArticles returns Seq with correct size"){
    val dao = new InMemoryDao(articleFile)
    val result1 = dao.getArticles(columns, Some(20))
    assert(result1.size === 20)
    val result2 = dao.getArticles(columns, Some(200))
    assert(result2.size === 100)
  }
}
