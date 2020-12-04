package model

import spray.json.{DefaultJsonProtocol, JsNumber, JsObject, JsString, JsValue, RootJsonFormat}

case class PosAnnotation(begin: Int, end: Int, tag: String){

  override def toString: String = "[begin: "+begin+", end: "+end+", tag: "+tag+"]"
}

object PosAnnotationJsonProtocol extends DefaultJsonProtocol{

  implicit object PosAnnotationJsonFormat extends RootJsonFormat[PosAnnotation] {

    override def read(json: JsValue): PosAnnotation = ???

    override def write(posAnnotation: PosAnnotation): JsObject = JsObject(
      "begin" -> JsNumber(posAnnotation.begin),
      "end" -> JsNumber(posAnnotation.end),
      "tag" -> JsString(posAnnotation.tag)
    )
  }
}
