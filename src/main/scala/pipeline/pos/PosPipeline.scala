package pipeline.pos

import com.johnsnowlabs.nlp.annotators.pos.perceptron.PerceptronModel
import com.johnsnowlabs.nlp.annotator.{LemmatizerModel, Normalizer, SentenceDetector, Tokenizer}
import com.johnsnowlabs.nlp.DocumentAssembler
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{DataFrame, SparkSession}
import pipeline.PipelineTrait

class PosPipeline(val spark: SparkSession) extends PipelineTrait{

  //use in dev mode
  //val lemmatizerModel = "src/main/resources/models/lemma_de_2.0.8_2.4_1561248996126"
  //val posModel = "src/main/resources/models/pos_ud_hdt_de_2.0.8_2.4_1561232528570"

  //use in prod mode
  val lemmatizerModel = "SparkNLP/resources/models/lemma_de_2.0.8_2.4_1561248996126"
  val posModel = "SparkNLP/resources/models/pos_ud_hdt_de_2.0.8_2.4_1561232528570"

  /*Clean everything but higher and lower case letters, including ä,ö,ü,ß,Ø,ø,ć - numbers and super/subscript.
  Dont't remove , . - / : if a number follows. Don't remove - if letter or number follows.
  Don't remove ' if letter follows*/
  val cleanUpPattern = ",(?!\\d)|\\:(?!\\d)|\\/(?!\\d)|-(?![A-Za-z\\d])|'(?![A-Za-z])|\\.(?!\\d)|[^A-Za-z-äöüÄÖÜßØøć0-9,.:/'\\u006E\\u00B0\\u00B2\\u00B3\\u00B9" +
    "\\u02AF\\u0670\\u0711\\u2121\\u213B\\u2207\\u29B5\\uFC5B-\\uFC5D\\uFC63\\uFC90\\uFCD9\\u2070\\u2071\\u2074-" +
    "\\u208E\\u2090-\\u209C\\u0345\\u0656\\u17D2\\u1D62-\\u1D6A\\u2A27\\u2C7C]"

  val splitPattern = "(?<=[^A-Z\\d])\\.\\b|[\\s ]"

  val documentAssembler = new DocumentAssembler()
    .setInputCol("text")
    .setOutputCol("document")

  val sentenceDetector = new SentenceDetector()
    .setInputCols("document")
    .setOutputCol("sentence")

  val tokenizer = new Tokenizer()
    .setInputCols(Array("sentence"))
    .setOutputCol("token")
    .setSplitPattern(splitPattern)

  val normalizer = new Normalizer()
    .setInputCols(Array("token"))
    .setOutputCol("normalized")
    .setCleanupPatterns(Array(cleanUpPattern))

  val lemmatizer = LemmatizerModel
    .load(lemmatizerModel)
    .setInputCols(Array("normalized"))
    .setOutputCol("lemma")

  val posTagger = PerceptronModel
    .load(posModel)
    .setInputCols(Array("sentence", "normalized"))
    .setOutputCol("pos")

  //useful for development
  /*val finisher = new Finisher()
    .setInputCols(Array("token", "normalized", "pos", "lemma"))
    .setCleanAnnotations(false)
    .setIncludeMetadata(false)*/

  val pipeline = new Pipeline()
    .setStages(Array(
      documentAssembler,
      sentenceDetector,
      tokenizer,
      normalizer,
      posTagger,
      lemmatizer
    ))

  override def train(articles: DataFrame, write: Option[String] = None ): PipelineModel = {
    val model = pipeline.fit(articles)
    write match {
      case None => model
      case Some(path) => {
        //writing the model will yield task size warning
        model.write.overwrite().save(path)
        model
      }
    }
  }

  override def annotate(articles: DataFrame, path: String): DataFrame = {
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
