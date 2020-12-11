package pipeline.pos

import com.johnsnowlabs.nlp.annotators.pos.perceptron.PerceptronModel
import com.johnsnowlabs.nlp.annotator.{LemmatizerModel, Normalizer, SentenceDetector, Tokenizer}
import com.johnsnowlabs.nlp.{DocumentAssembler, Finisher}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.{DataFrame, SparkSession}
import pipeline.PipelineTrait
import model.Strings

class PosPipeline(val spark: SparkSession, posModel: String) extends PipelineTrait{

  //val lemmatizer_model = "src/main/resources/lemma_de_2.0.8_2.4_1561248996126"

  import spark.implicits._

  val documentAssembler = new DocumentAssembler()
    .setInputCol(Strings.columnText)
    .setOutputCol(Strings.columnDocument)

  val sentenceDetector = new SentenceDetector()
    .setInputCols(Strings.columnDocument)
    .setOutputCol(Strings.columnSentence)

  val tokenizer = new Tokenizer()
    .setInputCols(Array(Strings.columnSentence))
    .setOutputCol(Strings.columnToken)
    .setSuffixPattern(Strings.tokenizerSuffixPattern)
    .setPrefixPattern(Strings.tokenizerPrefixPattern)
    .addException(Strings.exceptionKitt)

  //TODO nochmal evaluieren, ob pos tags besser mit oder ohne normalizer gefunden werden (zb Relativpronomen).
  //bei großen Differenzen, die Punct tags hinterher rausfiltern
  val normalizer = new Normalizer()
    .setInputCols(Array(Strings.columnToken))
    .setOutputCol(Strings.columnNormalized)

  //auf den 1. blick nicht wirklich zufriedenstellend
  /*val lemmatizer = LemmatizerModel
    .load(lemmatizer_model)
    .setInputCols(Array("normalized"))
    .setOutputCol("lemma")*/

  val posTagger = PerceptronModel
    .load(posModel)
    .setInputCols(Array(Strings.columnSentence, Strings.columnNormalized))
    .setOutputCol(Strings.columnPos)

  val finisher = new Finisher()
    .setInputCols(Strings.columnToken, Strings.columnNormalized, Strings.columnPos)
    .setCleanAnnotations(false)
    .setIncludeMetadata(false)

  val pipeline = new Pipeline()
    .setStages(Array(
      documentAssembler,
      sentenceDetector,
      tokenizer,
      normalizer,
      posTagger,
      finisher
    ))



   override def runPipeline(articles: DataFrame): DataFrame = {
    pipeline.fit(articles).transform(articles)
  }

  /*def replace(articlesDf: DataFrame,
              replacements: Map[String, String]): DataFrame = {
    var articlesVar = articlesDf
    for((pattern, replacement) <- replacements){
      articlesVar = articlesVar.withColumn(Strings.columnText,
        regexp_replace(articlesVar(Strings.columnText), pattern, replacement))
    }
    articlesVar
  }*/

  override def train(articles: DataFrame): PipelineModel = {
    //val data = articles.toDF(Strings.columnId, Strings.columnLongUrl, Strings.columnCrawlTime, Strings.columnText)
    //val dataEdited = replace(data, replacements)
    pipeline.fit(articles)
  }

  override def annotate(articles: DataFrame,
                        path: String): DataFrame = {
    //val data = articles.toDF(Strings.columnId, Strings.columnLongUrl, Strings.columnCrawlTime, Strings.columnText)
    //val dataEdited = replace(data, replacements)
    val model = PipelineModel.load(path)
    model.transform(articles)
  }
}
