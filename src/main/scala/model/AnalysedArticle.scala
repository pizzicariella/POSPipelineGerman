package model

import spray.json.{DefaultJsonProtocol, JsArray, JsNumber, JsObject, JsString, JsValue, RootJsonFormat}

case class AnalysedArticle(id: String,
                           longUrl: String,
                           crawlTime: BigDecimal,
                           text: String,
                           annotationsPos: List[PosAnnotation]) {
  /*def annosPosAsJsArray: Vector[JsArray] = annosPos.map(anno => JsArray(JsNumber(anno._1),
                                                                JsNumber(anno._2),
                                                                JsString(anno._3))).toVector*/

  def annotationsPosAsJsObject: Vector[JsObject] = annotationsPos
    .map(anno => PosAnnotationJsonProtocol.PosAnnotationJsonFormat.write(anno))
    .toVector


  override def toString: String = Strings.analysedArticleString(id, longUrl, crawlTime, text, annotationsPos)

}

object AnalysedArticleJsonProtocol extends DefaultJsonProtocol{

  implicit object AnalysedArticleJsonFormat extends RootJsonFormat[AnalysedArticle] {

    override def read(json: JsValue): AnalysedArticle = ???

    override def write(analysedArticle: AnalysedArticle): JsObject = JsObject(
        Strings.columnId -> JsObject(Map(Strings.fieldId -> JsString(analysedArticle.id))),
        "longUrl" -> JsString(analysedArticle.longUrl),
        "crawlTime" -> JsObject(Map(Strings.fieldDate -> JsNumber(analysedArticle.crawlTime))),
        Strings.columnText -> JsString(analysedArticle.text),
        "annotationsPos" -> JsArray(analysedArticle.annotationsPosAsJsObject)
      )
  }
}


