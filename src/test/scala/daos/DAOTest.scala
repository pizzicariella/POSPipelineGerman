package daos

import com.typesafe.config.ConfigFactory
import daos.memory.InMemoryDao
import org.scalatest.funsuite.AnyFunSuite

class DAOTest extends AnyFunSuite{

  val articleFile = ConfigFactory.load().getString("app.inmemoryfile_test")
  val columns = Array("title", "intro", "text")

  test("getArticles returns Seq with correct size"){
    val dao = new InMemoryDao()
    val result1 = dao.getNewsArticles(Some(20), articleFile)
    assert(result1.size === 20)
    val result2 = dao.getNewsArticles(Some(200), articleFile)
    assert(result2.size === 100)
  }
}
