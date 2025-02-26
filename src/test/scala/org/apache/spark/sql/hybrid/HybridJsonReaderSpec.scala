package org.apache.spark.sql.hybrid

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class HybridJsonReaderSpec extends AnyFlatSpec with should.Matchers {
  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[1]")
      .appName("test")
      .getOrCreate()

  "Reader" should "Read No schema" in {
    val df: DataFrame = spark.read
      .format("hybrid-json")
      .option("objectName", "test01")
      .load()

    df.show()
    println(df.schema)

  }

  "Reader" should "Read With Schema" in {
    val schema = StructType(Seq(StructField("id", StringType, nullable = false)))
    val df: DataFrame = spark.read
      .format("hybrid-json")
      .option("objectName", "test01")
      .schema(schema)
      .load()

    df.show()
    println(df.schema)
  }

}
