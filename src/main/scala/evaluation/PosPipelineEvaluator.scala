package evaluation

import org.apache.spark.sql.functions.{arrays_zip, col, expr}
import org.apache.spark.sql.{DataFrame, SparkSession}
import pipeline.pos.PosPipeline
import utils.Conversion

//TODO Test
class PosPipelineEvaluator(val spark: SparkSession) extends Evaluator {

  override def evaluateModel(testArticles: DataFrame, goldStandard: DataFrame, pathToModel: String): DataFrame ={
    val pipeline = new PosPipeline(spark, "src/main/resources/models/pos_ud_hdt_de_2.0.8_2.4_1561232528570")
    val preparedArticles = Conversion.prepareArticlesForPipeline(testArticles)
    val annotated = pipeline.annotate(preparedArticles, pathToModel)
    val annotatedFinal = Conversion.prepareArticlesForSaving(annotated)
    annotatedFinal
      .select("_id", "pos", "lemma")
      .join(goldStandard
        .select("_id", "pos", "lemma")
        .withColumnRenamed("pos", "pos_gold")
        .withColumnRenamed("lemma", "lemma_gold"), "_id")
      .withColumn("pos_zipped", arrays_zip(col("pos"), col("pos_gold")))
      .withColumn("lemma_zipped", arrays_zip(col("lemma"), col("lemma_gold")))
      .withColumn("pos_mapped",
        expr("transform(pos_zipped, x -> int(if(x.pos.tag == x.pos_gold.tag, 1, 0)))"))
      .withColumn("lemma_mapped",
        expr("transform(lemma_zipped, x -> int(if(x.lemma.result == x.lemma_gold.result, 1, 0)))"))
      .withColumn("accuracy_pos",
        expr("double(aggregate(pos_mapped, 0, (acc, val) -> acc + val)) / size(pos_mapped)"))
      .withColumn("accuracy_lemma",
        expr("double(aggregate(lemma_mapped, 0, (acc, val) -> acc + val)) / size(lemma_mapped)"))
      .select("_id","accuracy_pos", "accuracy_lemma")
  }
}
