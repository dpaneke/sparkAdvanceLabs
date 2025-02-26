package org.apache.spark.sql.hybrid

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
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

    val schema: StructType = data.schema
    val rdd: RDD[InternalRow] = data.queryExecution.toRdd

    rdd.foreachPartition { partition =>
      val mongoWriter: MongoPartitionHandler = new MongoPartitionHandler(
        mongoUri = WriterMetaConfig.mongoUri,
        dbName = WriterMetaConfig.mongoDbName,
        objectName = WriterMetaConfig.objectName(parameters)
      )
      val path: String = WriterMetaConfig.filesPath(parameters)
      val absPath = s"$path/${UUID.randomUUID().toString}.json"
      if (partition.hasNext) {
        val converter: RowConverter = new RowConverter(schema)
        FileHelper.writeIteratorData(absPath, converter.toJsonString(partition))
        mongoWriter.writeFileMeta(WriterMetaConfig.fileCollectionName, absPath)
        mongoWriter.writeSchemaMeta(WriterMetaConfig.schemaCollectionName, schema)
      }
      mongoWriter.closeConnection()
    }

    new BaseRelation {
      override def sqlContext: SQLContext = ???

      override def schema: StructType = ???
    }
  }

  override def shortName(): String = "hybrid-json"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val objectName: String = WriterMetaConfig.objectName(parameters)
    val mongoReader: MongoPartitionHandler = new MongoPartitionHandler(
      mongoUri = WriterMetaConfig.mongoUri,
      dbName = WriterMetaConfig.mongoDbName,
      objectName = objectName
    )
    val schemaInp: StructType = mongoReader.getSchema(WriterMetaConfig.schemaCollectionName, objectName)
    val partitionInfoSeq: Seq[PartitionInfo] = mongoReader.getPartitionInfo(
      WriterMetaConfig.fileCollectionName,
      objectName
    )
    new HybridJsonRelation(
      partitionInfoSeq,
      schemaInp
    )
  }

  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String],
    schema: StructType
  ): BaseRelation = {

    val objectName: String = WriterMetaConfig.objectName(parameters)

    val mongoReader: MongoPartitionHandler = new MongoPartitionHandler(
      mongoUri = WriterMetaConfig.mongoUri,
      dbName = WriterMetaConfig.mongoDbName,
      objectName = objectName
    )
    val partitionInfoSeq: Seq[PartitionInfo] = mongoReader.getPartitionInfo(
      WriterMetaConfig.fileCollectionName,
      objectName
    )
    new HybridJsonRelation(
      partitionInfoSeq,
      schema
    )
  }

}
