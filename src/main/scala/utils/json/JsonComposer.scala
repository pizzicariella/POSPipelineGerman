package utils.json

import spray.json._

object JsonComposer {

  def composeAnalysedArticleJson(id: String,
                                 longUrl: String,
                                 crawlTime: BigDecimal,
                                 text: String,
                                 annosPos: List[(Int, Int, String)]): String =
    AnalysedArticle(id, longUrl, crawlTime, text, annosPos)
      .toJson(AnalysedArticleJsonProtocol.AnalysedArticleJsonFormat)
      .compactPrint

}
