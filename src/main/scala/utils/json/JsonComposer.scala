package utils.json

import model.{AnalysedArticleJsonProtocol, AnnotatedArticle}
import spray.json._

object JsonComposer {

  def composeAnalysedArticleJson(article: AnnotatedArticle): String =
    article
      .toJson(AnalysedArticleJsonProtocol.AnnotatedArticleJsonFormat)
      .compactPrint

}
