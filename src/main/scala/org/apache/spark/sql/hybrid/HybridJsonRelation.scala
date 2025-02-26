package org.apache.spark.sql.hybrid

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.{LongType, StructField, StructType}

class HybridJsonRelation(
  partitionInfoSeq: Seq[PartitionInfo],
  schemaData: StructType
) extends BaseRelation with TableScan {

  override def sqlContext: SQLContext = SparkSession.active.sqlContext

  override def schema: StructType = {
    StructType(commitMillisField +: schemaData.fields)
  }

  private val commitMillisField: StructField = StructField(
    name = "__commitMillis",
    dataType = LongType,
    nullable = true
  )

  override def buildScan(): RDD[Row] = {
    new HybridJsonRdd(schemaData, partitionInfoSeq).asInstanceOf[RDD[Row]]
  }

  override def needConversion: Boolean = false
}
