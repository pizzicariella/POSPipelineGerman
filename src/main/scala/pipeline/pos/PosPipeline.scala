package pipeline.pos

import com.johnsnowlabs.nlp.annotators.pos.perceptron.PerceptronModel
import com.johnsnowlabs.nlp.annotator.{LemmatizerModel, Normalizer, SentenceDetector, Tokenizer}
import com.johnsnowlabs.nlp.{DocumentAssembler, Finisher}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, SparkSession}
import pipeline.PipelineTrait

class PosPipeline(val spark: SparkSession, posModel: String) extends PipelineTrait{

  val lemmatizer_model = "src/main/resources/models/lemma_de_2.0.8_2.4_1561248996126"
  val cleanUpPattern = ",(?!\\d)|-(?![A-Za-z])|\\.(?!\\d)|[^A-Za-z-äöüÄÖÜß0-9,.\\u006E\\u00B0\\u00B2\\u00B3\\u00B9" +
    "\\u02AF\\u0670\\u0711\\u2121\\u213B\\u2207\\u29B5\\uFC5B-\\uFC5D\\uFC63\\uFC90\\uFCD9\\u2070\\u2071\\u2074-" +
    "\\u208E\\u2090-\\u209C\\u0345\\u0656\\u17D2\\u1D62-\\u1D6A\\u2A27\\u2C7C]"

  val documentAssembler = new DocumentAssembler()
    .setInputCol("text")
    .setOutputCol("document")

  val sentenceDetector = new SentenceDetector()
    .setInputCols("document")
    .setOutputCol("sentence")

  //TODO check if patterns are arbitrary because of normalizer --> looks like it is, but not sure yet
  val tokenizer = new Tokenizer()
    .setInputCols(Array("sentence"))
    .setOutputCol("token")
    //.addSplitChars(" ")
    //TODO Dies sollte das replace in der Vorverarbeitung der Artikel unnötig machen (nochmal prüfen)
    .setSplitPattern("(?<=[^A-Z\\d])\\b\\.\\b|[\\s ]")
    //.setSuffixPattern("([^\\s\\w\\ü\\Ü\\ö\\Ö\\ä\\Ä\\ß\\Ø\\ø\\-]?)([^\\s\\w\\ü\\Ü\\ö\\Ö\\ä\\Ä\\ß\\Ø\\ø\\-]*)\\z")
    //.setPrefixPattern("\\A([^\\s\\w\\d\\ü\\Ü\\ö\\Ö\\ä\\Ä\\ß\\Ø\\ø\\-]?)([^\\s\\w\\d\\ü\\Ü\\ö\\Ö\\ä\\Ä\\ß\\Ø\\ø\\-]*)")

  //TODO nochmal evaluieren, ob pos tags besser mit oder ohne normalizer gefunden werden (zb Relativpronomen).
  //bei großen Differenzen, die Punct tags hinterher rausfiltern
  val normalizer = new Normalizer()
    .setInputCols(Array("token"))
    .setOutputCol("normalized")
    //Clean everything but higher and lower case letters, including ä,ö,ü,ß, - numbers and super/subscript.
    // Dont't remove , or . if a number follows. Don't remove - if letter follows
    .setCleanupPatterns(Array(cleanUpPattern))
    /*.setCleanupPatterns(Array("[^A-Za-z-äöüÄÖÜß0-9" +
      "\\u006E\\u00B0\\u00B2\\u00B3\\u00B9\\u02AF\\u0670\\u0711\\u2121\\u213B\\u2207\\u29B5\\uFC5B-\\uFC5D\\uFC63" +
      "\\uFC90\\uFCD9\\u2070\\u2071\\u2074-\\u208E\\u2090-\\u209C\\u0345\\u0656\\u17D2\\u1D62-\\u1D6A\\u2A27\\u2C7C]" +
      ""))*/

  //auf den 1. blick nicht wirklich zufriedenstellend
  val lemmatizer = LemmatizerModel
    .load(lemmatizer_model)
    .setInputCols(Array("normalized"))
    .setOutputCol("lemma")

  val posTagger = PerceptronModel
    .load(posModel)
    .setInputCols(Array("sentence", "normalized"))
    .setOutputCol("pos")

  val finisher = new Finisher()
    .setInputCols(Array("token", "normalized", "pos", "lemma"))
    .setCleanAnnotations(false)
    .setIncludeMetadata(false)

  val pipeline = new Pipeline()
    .setStages(Array(
      documentAssembler,
      sentenceDetector,
      tokenizer,
      normalizer,
      posTagger,
      lemmatizer,
      finisher
    ))

  override def train(articles: DataFrame, write: Option[String] = None ): PipelineModel = {
    val model = pipeline.fit(articles)
    write match {
      case None => model
      case Some(path) => {
        //writing the model will yield task size warning, but is necessary for demo page
        model.write.overwrite().save(path)
        model
      }
    }
  }

  override def annotate(articles: DataFrame,
                        path: String): DataFrame = {
    val model = PipelineModel.load(path)
    model.transform(articles)
  }

  def runPipeline(articles: DataFrame, path: Option[String] = None): DataFrame = {
    val model = pipeline.fit(articles)
    path match {
      case Some(p) => {
        model.write.overwrite().save(p)
        model.transform(articles)
      }
      case None => model.transform(articles)
    }
  }
}
