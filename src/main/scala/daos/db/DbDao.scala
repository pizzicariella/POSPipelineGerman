package daos.db

import com.mongodb.spark.MongoSpark
import daos.DAO
import org.apache.spark.sql.{DataFrame, SparkSession}

class DbDao(val spark: SparkSession) extends DAO{

  override def getNewsArticles(limit: Option[Int]): DataFrame = {

    val articles = MongoSpark.load(spark).toDF()
      .drop("short_url",
        "keywords",
        "published_time",
        "news_site",
        "image_links",
        "description",
        "authors",
        "links")

    limit match {
      case Some(x) => articles.limit(x)
      case None => articles
    }
  }

  /**
   * Writes multiple analysed articles to destination
   *
   * @param articles
   * @param destination
   */
  override def writeArticles(articles: DataFrame): Unit = {
    MongoSpark.save(articles)
  }
}
