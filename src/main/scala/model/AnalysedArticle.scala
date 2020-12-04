package model

import spray.json.{DefaultJsonProtocol, JsArray, JsObject, JsString, JsValue, RootJsonFormat}

case class AnalysedArticle(id: String,
                           longUrl: String,
                           crawlTime: String,
                           text: String,
                           annotationsPos: List[PosAnnotation]) {

  def annotationsPosAsJsObject: Vector[JsObject] = annotationsPos
    .map(anno => PosAnnotationJsonProtocol.PosAnnotationJsonFormat.write(anno))
    .toVector

  override def toString: String = Strings.analysedArticleString(id, longUrl, crawlTime, text, annotationsPos)
}

object AnalysedArticleJsonProtocol extends DefaultJsonProtocol{

  implicit object AnalysedArticleJsonFormat extends RootJsonFormat[AnalysedArticle] {

    override def read(json: JsValue): AnalysedArticle = ???

    override def write(analysedArticle: AnalysedArticle): JsObject = JsObject(
        "_id" -> JsString(analysedArticle.id),
        "longUrl" -> JsString(analysedArticle.longUrl),
        "crawlTime" -> JsString(analysedArticle.crawlTime),
        Strings.columnText -> JsString(analysedArticle.text),
        "annotationsPos" -> JsArray(analysedArticle.annotationsPosAsJsObject)
      )
  }
}


