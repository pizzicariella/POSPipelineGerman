package utils

object Utils {

  //TODO how to handle non string columns?
  def getArticleWithCompleteText(articleMap: Map[String, Any],
                                 textColumns: Array[String],
                                 otherRelevantColumns: Array[String]): (Array[String], String) = {

    val text = textColumns.map(columnName =>
      articleMap.getOrElse(columnName,
        throw new NoSuchElementException("The column " + columnName + " does not exist.")) match {
        case s: String => s
        case _ => ""
      })
      .reduce((s1, s2) => s1 + " " + s2)

    (otherRelevantColumns.map(columnName =>
      articleMap.getOrElse(columnName,
        throw new NoSuchElementException("The column " + columnName + " does not exist.")).toString), text)
  }


}
