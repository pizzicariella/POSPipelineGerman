package utils

import java.io.FileNotFoundException

import com.typesafe.config.ConfigFactory
import org.scalatest.funsuite.AnyFunSuite
import model.Strings

class FileReaderTest extends AnyFunSuite{

  val path = ConfigFactory.load().getString(Strings.configTestFile)

  test("readJsonFile should return list of lines"){
    val jsons = FileReader.readJsonFile(path)
    assert(jsons.isInstanceOf[List[String]])
    assert(jsons.size === 100)
  }

  test("readJsonFile should throw FileNotFoundException on not existing path"){
    assertThrows[FileNotFoundException]{
      FileReader.readJsonFile("src/test/resources/notExistingFile")
    }
  }

}
