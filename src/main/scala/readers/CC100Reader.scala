package ua.nlp.ukrlm
package readers

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD

object CC100Reader {
  def apply(spark: SparkSession, path: String): RDD[Map[String, String]] = {
    val lines = spark.sparkContext.textFile(path)

    val rdd = lines
      .filter(_.nonEmpty)
      .zipWithIndex
      .map { case (line, idx) =>
        val id = idx.toString
        val text = line
        Map("id" -> id, "text" -> text)
      }

    rdd
  }
}
