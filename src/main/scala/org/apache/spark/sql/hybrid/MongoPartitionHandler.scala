package org.apache.spark.sql.hybrid

import org.apache.spark.sql.types.{DataType, StructType}
import org.mongodb.scala.{MongoClient, MongoCollection, MongoDatabase, Completed}
import org.mongodb.scala.model.Filters.equal
import org.mongodb.scala.bson.Document

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
class MongoPartitionHandler(
  mongoUri: String,
  dbName: String,
  objectName: String
) {

  val client: MongoClient = MongoClient(mongoUri)
  val db: MongoDatabase = client.getDatabase(dbName)

  def writeFileMeta(collectionName: String, path: String): Unit = {
    val collection: MongoCollection[Document] = db.getCollection(collectionName)
    val commitMillis: Long = System.currentTimeMillis
    val document: Document = Document(
      "objectName" -> objectName,
      "path" -> path,
      "commitMillis" -> commitMillis
    )
    val insertFuture: Future[Completed] = collection.insertOne(document).toFuture()
    Await.result(insertFuture, 10.seconds)
  }

  def writeSchemaMeta(collectionName: String, schema: StructType): Unit = {
    val collection: MongoCollection[Document] = db.getCollection(collectionName)
    val firstDoc: Future[Document] = collection.find(equal("objectName", objectName)).first().toFuture()
    val isSchemaWritten: Boolean = Await.result(firstDoc, 10.seconds) match {
      case _: Document => true
      case null => false
    }
    if (!isSchemaWritten) {
      val schemaRef: String = schema.asNullable.json
      val commitMillis: Long = System.currentTimeMillis
      val document: Document = Document(
        "objectName" -> objectName,
        "schemaRef" -> schemaRef,
        "commitMillis" -> commitMillis
      )
      val insertFuture: Future[Completed] = collection.insertOne(document).toFuture()
      Await.result(insertFuture, 10.seconds)
    }
  }

  def getPartitionInfo(collectionName: String, objectName: String): Seq[PartitionInfo] = {
      val collection: MongoCollection[Document] = db.getCollection(collectionName)
      val docsFuture: Future[Seq[Document]] = collection.find(equal("objectName", objectName)).toFuture()
      val docsSeq: Seq[Document] = Await.result(docsFuture, 10.seconds)
      docsSeq.map { doc =>
        PartitionInfo(
          path = doc.get("path").map(_.asString().getValue).get,
          commitMillis = doc.get("commitMillis").map(_.asInt64().getValue).get
        )
      }
    }

  def getSchema(collectionName: String, objectName: String): StructType = {
      val collection: MongoCollection[Document] = db.getCollection(collectionName)
      val firstDoc: Future[Document] = collection.find(equal("objectName", objectName)).first().toFuture()
      val jsonSchema: String = Await.result(firstDoc, 10.seconds)
        .get("schemaRef")
        .map(_.asString().getValue)
        .get
      DataType.fromJson(jsonSchema).asInstanceOf[StructType]
    }

  def closeConnection(): Unit = client.close()

}