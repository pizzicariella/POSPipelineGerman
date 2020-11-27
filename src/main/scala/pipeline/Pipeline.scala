package pipeline

import org.apache.spark.sql.DataFrame

trait PipelineTrait {
  /**
   * Runs and/or trains the pipeline.
   * @return A DataFrame containing annotations.
   */
  def runPipeline(articles: Seq[(String, String, String, String)],
                  replacePatternSplitChars: Option[String],
                  replacement: Option[String]): DataFrame
}
