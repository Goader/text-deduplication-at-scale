package ua.nlp.ukrlm

import readers.CC100Reader
import deduplication.MinhashLSH

object Main {
  def main(args: Array[String]): Unit = {
    val spark = org.apache.spark.sql.SparkSession.builder
      .master("local")
      .appName("text-deduplication")
      .getOrCreate

    val cc100 = CC100Reader(spark, "/media/goader/masters/corpora/cc-100/uk-sample.txt")
    cc100.take(10).foreach(println)

    val minhashLSH = MinhashLSH(
      contentCol = "text",
      idCol = "id",
      numPerm = 32,
      ngramSize = 3,
      bandsCount = 10,
      rowsCount = 3,
      minNgramSize = 3,
      seed = 0
    )

    val cc100Bands = minhashLSH.run(cc100)
    cc100Bands.take(30).foreach(println)
  }
}