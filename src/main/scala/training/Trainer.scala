package training

import model.NewsArticle
import org.apache.spark.ml.PipelineModel
import utils.json.AnalysedArticle

trait Trainer {

  /**
   * Starts training and returns trained model. If you wish to save model pass Some(pathToSaveModel) else None.
   * @param path
   * @return
   */
  def startTraining(path: Option[String]): PipelineModel

  /**
   * Get results from training.
   * @param articles Pass None if you want to use the same articles as in training
   * @param path to load model
   * @param save pass true if results should be saved to db
   * @return
   */
  def results(articles: Option[Seq[NewsArticle]], path: String, save: Boolean): Seq[AnalysedArticle]

}
