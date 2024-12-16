package org.apache.spark.sql.hybrid

import org.apache.spark.sql.SparkSession

object Main extends App {
  val spark: SparkSession = SparkSession.builder()
    .master("local")
    .getOrCreate()

  val df = spark.range(0, 10)
  df.show()

}
