package org.apache.spark.sql.hybrid

import org.apache.spark.sql.types.StructType
import org.mongodb.scala.Completed
import org.mongodb.scala.{MongoClient, MongoCollection, MongoDatabase}
import org.mongodb.scala.model.Filters.equal
import org.apache.spark.sql.types.StructType
import org.mongodb.scala.bson.Document

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
class MongoPartitionWriter(
  val mongoUri: String,
  val dbName: String,
  val objectName: String
) {
  private def withConnection(blockFunc: MongoDatabase => Unit): Unit = {
    val client: MongoClient = MongoClient(mongoUri)
    val db: MongoDatabase = client.getDatabase(dbName)
    blockFunc(db)
    client.close()
  }

  def writeFileMeta(collectionName: String, path: String): Unit = withConnection { db =>
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

  def writeSchemaMeta(collectionName: String, schema: StructType): Unit = withConnection { db =>
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
}