package org.apache.spark.sql.hybrid

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.StructType

import java.io.File
import scala.io.BufferedSource

class HybridJsonRdd(
  schema: StructType,
  partitionInfoSeq: Seq[PartitionInfo]
) extends RDD[InternalRow](SparkSession.active.sparkContext, Nil) {
  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val hybridJsonPartition: HybridJsonPartition = split.asInstanceOf[HybridJsonPartition]
    val buffSource: BufferedSource = scala.io.Source.fromFile(hybridJsonPartition.path)
    val jsonIter: Iterator[String] = buffSource.getLines()
    new JsonParser(schema).toRow(jsonIter).map { row =>
      new GenericInternalRow(hybridJsonPartition.commitMillis +: row.toSeq(schema).toArray)
        .asInstanceOf[InternalRow]
    }
  }

  override protected def getPartitions: Array[Partition] = {
    val paths: Seq[String] = partitionInfoSeq.map(_.path)
    val commitMillisSeq: Seq[Long] = partitionInfoSeq.map(_.commitMillis)
    val files: Array[File] = paths
      .map(path => new File(path))
      .filter(file => file.length() > 0)
      .toArray
    files.zip(commitMillisSeq).zipWithIndex.map {
      case ((file, commitMillis), idx) => HybridJsonPartition(idx, file.getAbsolutePath, commitMillis)
    }
  }
}
