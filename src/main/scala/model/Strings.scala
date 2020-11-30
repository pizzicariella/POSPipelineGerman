package model

object Strings {
  val columnId = "_id"
  val columnLongUrl = "long_url"
  val columnCrawlTime = "crawl_time"
  val columnTitle = "title"
  val columnIntro = "intro"
  val columnText = "text"
  val columnCompleteText = "completeText"
  val columnDocument = "document"
  val columnSentence = "sentence"
  val columnToken = "token"
  val columnNormalized = "normalized"
  val columnPos = "pos"

  val fieldDate = "$date"
  val fieldId = "$oid"

  val mongoString = (serverAddress: String,
                     userName: String,
                     pw: String,
                     port: String,
                     db: String) => "mongodb://"+userName+":"+pw+"@"+serverAddress+":"+port+"/"+db

  val sparkParamsAppName = "POSPipelineGerman"
  val sparkParamsLocal = "local[*]"

  val configEvalFile = "app.file_eval"
  val configEvalPosTags = "app.pos_tags_eval"

  val tokenizerSuffixPattern = "([^\\s\\w\\ü\\Ü\\ö\\Ö\\ä\\Ä\\ß\\Ø\\ø\\-]?)([^\\s\\w\\ü\\Ü\\ö\\Ö\\ä\\Ä\\ß\\Ø\\ø\\-]*)\\z"
  val tokenizerPrefixPattern = "\\A([^\\s\\w\\d\\ü\\Ü\\ö\\Ö\\ä\\Ä\\ß\\Ø\\ø\\-]?)([^\\s\\w\\d\\ü\\Ü\\ö\\Ö\\ä\\Ä\\ß\\Ø\\ø\\-]*)"

  val exceptionKitt = "K.I.T.T."
}
