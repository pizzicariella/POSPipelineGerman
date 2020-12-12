package utils.json

import model.{AnalysedArticleJsonProtocol, AnnotatedArticle}
import spray.json._


//TODO delete if not needed any longer
object JsonComposer {

  def composeAnalysedArticleJson(article: AnnotatedArticle): String =
    article
      .toJson(AnalysedArticleJsonProtocol.AnnotatedArticleJsonFormat)
      .compactPrint

}
