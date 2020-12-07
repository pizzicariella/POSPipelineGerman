package utils

import java.io.{BufferedWriter, File, FileWriter}

import scala.io.Source

object FileIO {

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

  /**
   * Writes sequence of json Strings to file. Each json will be one line in file.
   * @param path
   * @param jsons
   */
    //TODO test
  def writeJsonFile(path: String, jsons: Seq[String]) = {
    val file = new File(path)
    val bw = new BufferedWriter(new FileWriter(file))
    jsons.foreach(json => bw.write(json))
    bw.close()
  }
}
