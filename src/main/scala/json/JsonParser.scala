package json

import spray.json.DefaultJsonProtocol._
import spray.json._

object JsonParser {

  //TODO: convert non string columns into strings?
  def parseDocumentText(json: String, columns: Array[String]) : Map[String, String] = {
    val jsonAst = json.parseJson
    val data = jsonAst.convertTo[Map[String, JsValue]]
    data.filter(entry => columns.contains(entry._1))
        .map(entry => (entry._1, entry._2 match {
            case JsString(value) => value
            case _ => ""
        }))
  }
}
