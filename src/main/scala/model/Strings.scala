package model

object Strings {
  val columnId = "_id"
  val columnLongUrl = "long_url"
  val columnCrawlTime = "crawl_time"
  val columnTitle = "title"
  val columnIntro = "intro"
  val columnText = "text"
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
  val sparkConigExecuterMemory = "spark.executor.memory"
  val sparkConfigDriverMemory = "spark.driver.memory"
  val sparkParamsMemory = "12g"

  val tokenizerSuffixPattern = "([^\\s\\w\\ü\\Ü\\ö\\Ö\\ä\\Ä\\ß\\Ø\\ø\\-]?)([^\\s\\w\\ü\\Ü\\ö\\Ö\\ä\\Ä\\ß\\Ø\\ø\\-]*)\\z"
  val tokenizerPrefixPattern = "\\A([^\\s\\w\\d\\ü\\Ü\\ö\\Ö\\ä\\Ä\\ß\\Ø\\ø\\-]?)([^\\s\\w\\d\\ü\\Ü\\ö\\Ö\\ä\\Ä\\ß\\Ø\\ø\\-]*)"

  val replacePatternSpecialWhitespaces = " "
  val replacementWhitespaces = " "
  val replacePatternMissingWhitespaces = "(?<=[^A-Z\\d])\\b\\.\\b"
  val replacementMissingWhitespaces = ". "

  val exceptionKitt = "K.I.T.T."

  val configEvalFile = "app.file_eval"
  val configEvalPosTags = "app.pos_tags_eval"

  val dbConfigUser = "app.user"
  val dbConfigPw = "app.pw"
  val dbConfigServer = "app.server"
  val dbConfigPort = "app.port"
  val dbConfigDb = "app.db"
  val dbConfigCollection = "app.collection"

  val targetDbConfigUser = "app.target_user"
  val targetDbConfigPw = "app.target_pw"
  val targetDbConfigServer = "app.target_server"
  val targetDbConfigPort = "app.target_port"
  val targetDbConfigDb = "app.target_db"
  val targetDbConfigCollection = "app.target_collection"

  val configPosModel = "app.pos_tagger_model"
  val configPosPipelineModel = "app.pipeline_model"

}
