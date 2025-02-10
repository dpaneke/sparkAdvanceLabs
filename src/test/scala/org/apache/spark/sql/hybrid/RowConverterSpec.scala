package org.apache.spark.sql.hybrid

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.mongodb.scala._
import org.bson.Document

import java.lang
import scala.concurrent.Await
import scala.concurrent.duration._

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
    val df = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      Row(1, "Alice", "x"),
      Row(10, "Bob", "y"),
      Row(100, "John", "y")
    )),
      schema=StructType(
        Seq(StructField("id", IntegerType), StructField("name", StringType), StructField("xy", StringType))
      ))

//      val rdd: RDD[InternalRow] = df.queryExecution.toRdd
//      var outList: List[Iterator[String]] = Nil
//
//      rdd.foreachPartition { p =>
//        val converter: RowConverter = new RowConverter(df.schema)
//        val out: Iterator[String] = converter.toJsonString(p)
//        outList = out :: outList
//      }
//      println(outList)
    val rows: Iterator[InternalRow] = df.queryExecution.toRdd.toLocalIterator
    val converter: RowConverter = new RowConverter(df.schema)
    val out: Iterator[String] = converter.toJsonString(rows)
    println(out.toList)
    out.toList.foreach(println)
  }

  "Converter" should "write to Mongo" in {
    val df = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      Row(1, "Alice", "x"),
      Row(10, "Bob", "y"),
      Row(100, "John", "y")
    )),
      schema=StructType(
        Seq(StructField("id", IntegerType), StructField("name", StringType), StructField("xy", StringType))
      ))

    //      val rdd: RDD[InternalRow] = df.queryExecution.toRdd
    //      var outList: List[Iterator[String]] = Nil
    //
    //      rdd.foreachPartition { p =>
    //        val converter: RowConverter = new RowConverter(df.schema)
    //        val out: Iterator[String] = converter.toJsonString(p)
    //        outList = out :: outList
    //      }
    //      println(outList)

//    val client: MongoClient = MongoClient("mongodb+srv://Cluster26228:Tkl0c35KX2ZL@cluster26228.vlwp7.mongodb.net/")
//    val db: MongoDatabase = client.getDatabase("fromscala")
//    val collection: MongoCollection[Document] = db.getCollection("testcollection01")
//    val document: Document =  Document.parse(jsonString)

    val rdd: RDD[InternalRow]  = df.queryExecution.toRdd
    val schema: StructType = df.schema

    rdd.foreachPartition { partition =>
      val client: MongoClient = MongoClient("mongodb+srv://Cluster26228:Tkl0c35KX2ZL@cluster26228.vlwp7.mongodb.net/")
      val db: MongoDatabase = client.getDatabase("fromscala")
      val collection: MongoCollection[Document] = db.getCollection("testcollection01")
      val converter: RowConverter = new RowConverter(schema)

      converter.toJsonString(partition).foreach { jsonString =>
        val insertFuture = collection.insertOne(Document.parse(jsonString)).toFuture()
        Await.result(insertFuture, 10.seconds)
      }
      client.close()
    }


//    val rows: Iterator[InternalRow] = df.queryExecution.toRdd.toLocalIterator
//    val converter: RowConverter = new RowConverter(df.schema)
//    val out: Iterator[String] = converter.toJsonString(rows)
//    println(out.toList)
//    out.toList.foreach(println)
  }
}