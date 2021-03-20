package evaluation

import org.apache.spark.sql.functions.{arrays_zip, col, expr}
import org.apache.spark.sql.DataFrame

class PosPipelineEvaluator() extends Evaluator {

  override def evaluateModel(testArticles: DataFrame, goldStandard: DataFrame): DataFrame ={

    testArticles
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

  override def evaluationForTags(testArticles: DataFrame, goldStandard: DataFrame, posTagList: List[String]): DataFrame = {

    val filterExpr = posTagList
        .foldLeft("filter(pos_zipped, x -> ")((str, tag) => str+"x.pos_gold.tag == \""+tag+"\" or ")
        .dropRight(3) + ")"

    testArticles.select("_id", "pos")
      .join(goldStandard
        .select("_id", "pos")
        .withColumnRenamed("pos", "pos_gold"), "_id")
      .withColumn("pos_zipped", arrays_zip(col("pos"), col("pos_gold")))
      .withColumn("pos_zipped", expr(filterExpr))
      .withColumn("pos_mapped",
        expr("transform(pos_zipped, x -> int(if(x.pos.tag == x.pos_gold.tag, 1, 0)))"))
      .withColumn("recall_pos_selected",
        expr("double(aggregate(pos_mapped, 0, (acc, val) -> acc + val)) / size(pos_mapped)"))
      .select("_id", "recall_pos_selected")
  }
}
