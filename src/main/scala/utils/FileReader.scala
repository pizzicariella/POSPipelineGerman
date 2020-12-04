package utils

import scala.io.Source

object FileReader {

  /**
   * Reads a file and returns a list of lines. Should be used for files in which each line represents a json String.
   * @param path
   * @return
   */
  def readJsonFile(path: String): List[String] = {
    val bufferedSource = Source.fromFile(path)
    val lines = bufferedSource.getLines().toList
    bufferedSource.close()
    lines
  }
}
