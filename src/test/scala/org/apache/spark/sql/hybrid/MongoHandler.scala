package org.apache.spark.sql.hybrid

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.bson.Document
import org.mongodb.scala._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

class MongoHandler extends AnyFlatSpec with should.Matchers {

  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[1]")
      .appName("test")
      .getOrCreate()

  "Converter" should "write to Mongo" in {
    val df = spark.createDataFrame(spark.sparkContext.parallelize(Seq(
      Row(1, "Alice", "x110"),
      Row(10, "Bob", "y2134"),
      Row(100, "John", "y3e240-1")
    )),
      schema=StructType(
        Seq(StructField("id", IntegerType), StructField("name", StringType), StructField("xy", StringType))
      ))

    val rdd: RDD[InternalRow]  = df.queryExecution.toRdd
    val schema: StructType = df.schema

    rdd.foreachPartition { partition =>
      val mongoUri: String = sys.env.getOrElse(
        "MONGO_URI",
        throw new IllegalArgumentException("Must specify MONGO_URI")
      )
      val client: MongoClient = MongoClient(mongoUri)
      val db: MongoDatabase = client.getDatabase("fromscala")
      val collection: MongoCollection[Document] = db.getCollection("testcollection01")
      val converter: RowConverter = new RowConverter(schema)

      converter.toJsonString(partition).foreach { jsonString =>
        val insertFuture: Future[Completed] = collection.insertOne(Document.parse(jsonString)).toFuture()
        Await.result(insertFuture, 10.seconds)
      }
      client.close()
    }
  }

//  "Converter" should "write to Mongo" in {
//    val schema=StructType(
//      Seq(StructField("id", IntegerType), StructField("name", StringType), StructField("xy", StringType))
//    )
//
//    val jsonParser: JsonParser = new JsonParser(schema)
//
//    val client: MongoClient = MongoClient("mongodb+srv://Cluster26228:Tkl0c35KX2ZL@cluster26228.vlwp7.mongodb.net/")
//    val db: MongoDatabase = client.getDatabase("fromscala")
//    val collection: MongoCollection[Document] = db.getCollection("testcollection01")
//
//  collection.find().toFuture().map()
//
//    rdd.foreachPartition { partition =>
//      val client: MongoClient = MongoClient("mongodb+srv://Cluster26228:Tkl0c35KX2ZL@cluster26228.vlwp7.mongodb.net/")
//      val db: MongoDatabase = client.getDatabase("fromscala")
//      val collection: MongoCollection[Document] = db.getCollection("testcollection01")
//      val converter: RowConverter = new RowConverter(schema)
//
//      converter.toJsonString(partition).foreach { jsonString =>
//        val insertFuture = collection.insertOne(Document.parse(jsonString)).toFuture()
//        Await.result(insertFuture, 10.seconds)
//      }
//      client.close()
//    }
//  }

}
