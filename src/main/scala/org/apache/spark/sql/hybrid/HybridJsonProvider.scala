package org.apache.spark.sql.hybrid

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, hybrid}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType

import java.util.UUID

class HybridJsonProvider
  extends CreatableRelationProvider
  with DataSourceRegister
  with RelationProvider
  with SchemaRelationProvider {

  override def createRelation(
    sqlContext: SQLContext,
    mode: SaveMode,
    parameters: Map[String, String],
    data: DataFrame
  ): BaseRelation = {
    val path: String = parameters.getOrElse(
      "path",
      throw new IllegalArgumentException("Must specify path"))
    val objectName: String = parameters.getOrElse(
      "objectName",
      throw new IllegalArgumentException("Must specify objectName")
    )
    val mongoUri: String = sys.env.getOrElse(
      "MONGO_URI",
      throw new IllegalArgumentException("Must specify MONGO_URI")
    )
    val mongoDbName: String = "index_store"
    val fileCollectionName: String = "file_index"
    val schemaCollectionName: String = "schema_index"

    val schema: StructType = data.schema
    val rdd: RDD[InternalRow] = data.queryExecution.toRdd

    rdd.foreachPartition { partition =>
      val absPath = s"$path/${UUID.randomUUID().toString}.json"
      if (partition.hasNext) {
        val converter: RowConverter = new RowConverter(schema)
        val mongoWriter: MongoPartitionWriter = new MongoPartitionWriter(
          mongoUri = mongoUri, dbName = mongoDbName, objectName = objectName
        )
        FileHelper.writeIteratorData(absPath, converter.toJsonString(partition))
        mongoWriter.writeFileMeta(fileCollectionName, absPath)
        mongoWriter.writeSchemaMeta(schemaCollectionName, schema)
      }
    }

    new BaseRelation {
      override def sqlContext: SQLContext = ???

      override def schema: StructType = ???
    }
  }

  override def shortName(): String = "hybrid-json"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = ???

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = ???
}
