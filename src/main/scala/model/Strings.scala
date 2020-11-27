package model

object Strings {
  val columnId = "_id"
  val columnLongUrl = "long_url"
  val columnCrawlTime = "crawl_time"
  val columnTitle = "title"
  val columnIntro = "intro"
  val columnText = "text"

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
}
