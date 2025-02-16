package org.apache.spark.sql.hybrid

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.apache.log4j.Logger

import java.lang

class RowConverterSpec extends AnyFlatSpec with should.Matchers {

  val log = Logger.getLogger(getClass)

  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[1]")
      .appName("test")
      .getOrCreate()

  "Converter" should "work" in {
    val data: Dataset[lang.Long] = spark.range(3)
    val rows: Iterator[InternalRow] = data.queryExecution.toRdd.collect().toIterator
    val converter: RowConverter = new RowConverter(data.schema)
    val out: Iterator[String] = converter.toJsonString(rows)
    out.toList.foreach(println)
  }

  "Converter" should "convert Df" in {
    val df: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      Row(1, "Alice", "x"),
      Row(10, "Bob", "y"),
      Row(100, "John", "y")
    )),
      schema=StructType(
        Seq(StructField("id", IntegerType), StructField("name", StringType), StructField("xy", StringType))
      ))

    val rows: Iterator[InternalRow] = df.queryExecution.toRdd.toLocalIterator
    val converter: RowConverter = new RowConverter(df.schema)
    val out: Iterator[String] = converter.toJsonString(rows)
    println(out.toList)
    out.toList.foreach(println)
  }

}