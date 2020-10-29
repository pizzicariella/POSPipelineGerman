package pipeline.pos

import com.johnsnowlabs.nlp.annotators.pos.perceptron.PerceptronModel
import com.johnsnowlabs.nlp.annotator.{SentenceDetector, Tokenizer}
import com.johnsnowlabs.nlp.{DocumentAssembler, Finisher}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.{DataFrame, SparkSession}
import pipeline.PipelineTrait

class PosPipeline(val spark: SparkSession) extends PipelineTrait{

  val pos_tagger_model = "src/main/resources/pos_ud_hdt_de_2.0.8_2.4_1561232528570"

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

  val posTagger = PerceptronModel
    .load(pos_tagger_model)
    .setInputCols(Array("sentence", "token"))
    .setOutputCol("pos")

  val finisher = new Finisher()
    .setInputCols("sentence", "token", "pos")
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

   override def runPipeline(articles: Seq[String]): DataFrame = {
    val data = articles.toDF("text")
    pipeline.fit(data).transform(data)
  }

}
