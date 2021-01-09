package daos.memory

import java.io.File
import daos.DAO
import model.AnnotatedArticle
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{LongType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.reflect.io.Directory


class InMemoryDao(val spark: SparkSession, val sourceFile: String, val destinationFile: String) extends DAO{

  override def getNewsArticles(limit: Option[Int]): DataFrame = {

    val articles = spark.read.json(sourceFile)
      .drop("short_url",
        "keywords",
        "published_time",
        "news_site",
        "image_links",
        "description",
        "authors",
        "links")
      .withColumn("crawl_time", col("crawl_time.$date.$numberLong")
        .divide(1000)
        .cast(LongType)
        .cast(TimestampType))

    limit match {
      case Some(x) => articles.limit(x)
      case None => articles
    }
  }

  override def writeArticle(article: AnnotatedArticle, destination: String): Unit = ???

  /**
   * Writes multiple analysed articles to destination
   *
   * @param articles
   * @param destination
   */

  override def writeArticles(articles: DataFrame): Unit = {
      new Directory(new File(destinationFile)).deleteRecursively()
      articles.write.json(destinationFile)
  }
}

