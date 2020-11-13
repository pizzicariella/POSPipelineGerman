package pipeline.pos

import com.johnsnowlabs.nlp.annotators.pos.perceptron.PerceptronModel
import com.johnsnowlabs.nlp.annotator.{LemmatizerModel, Normalizer, SentenceDetector, Tokenizer}
import com.johnsnowlabs.nlp.{DocumentAssembler, Finisher}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.functions.{col, regexp_replace}
import org.apache.spark.sql.{DataFrame, SparkSession}
import pipeline.PipelineTrait

class PosPipeline(val spark: SparkSession) extends PipelineTrait{

  val pos_tagger_model = "src/main/resources/pos_ud_hdt_de_2.0.8_2.4_1561232528570"
  val lemmatizer_model = "src/main/resources/lemma_de_2.0.8_2.4_1561248996126"

  import spark.implicits._

  val documentAssembler = new DocumentAssembler()
    .setInputCol("text")
    .setOutputCol("document")

  val sentenceDetector = new SentenceDetector()
    .setInputCols("document")
    .setOutputCol("sentence")

  val tokenizer = new Tokenizer()
    .setInputCols(Array("sentence"))
    .setOutputCol("token")
    //.addSplitChars(" ")
    .setSuffixPattern("([^\\s\\w\\ü\\ö\\ä\\ß]?)([^\\s\\w\\ü\\ö\\ä\\ß]*)\\z")
    //.setSplitPattern("[ ]")
    //.setContextChars(Array("\"", ".", ",", "!", "?", ":"))
    //.setInfixPatterns(Array("([^\\s\\w\\ü\\ö\\ä\\ß\\-]?)", "([^\\s\\w\\ü\\ö\\ä\\ß\\-]*)"))
    //.setPrefixPattern("\\A([^\\s\\w]*)")

  /*val normalizer = new Normalizer()
    .setInputCols(Array("token"))
    .setOutputCol("normalized")

  //auf den 1. blick nicht wirklich zufriedenstellend
  val lemmatizer = LemmatizerModel
    .load(lemmatizer_model)
    .setInputCols(Array("normalized"))
    .setOutputCol("lemma")*/

  val posTagger = PerceptronModel
    .load(pos_tagger_model)
    .setInputCols(Array("sentence", "token"))
    .setOutputCol("pos")

  val finisher = new Finisher()
    .setInputCols("token", "pos")
    .setCleanAnnotations(false)
    .setIncludeMetadata(false)

  val pipeline = new Pipeline()
    .setStages(Array(
      documentAssembler,
      sentenceDetector,
      tokenizer,
      posTagger,
      finisher
    ))

   override def runPipeline(articles: Seq[String],
                            replacePatternSplitChars: Option[String],
                            replacement: Option[String]): DataFrame = {
    val data = articles.toDF("text")
    val dataEdited = replaceSplitChars(data, replacePatternSplitChars, replacement)
    pipeline.fit(dataEdited).transform(dataEdited)
  }

  def replaceSplitChars(articlesDf: DataFrame,
                        replacePatternSplitChars: Option[String],
                        replacement: Option[String]): DataFrame = {
    articlesDf.withColumn("text", replacePatternSplitChars match {
      case Some(x) => regexp_replace(articlesDf("text"), x, replacement match {
        case Some(x) => x
        case _ => " "
      })
      case _ => col("text")
    })
  }

}
