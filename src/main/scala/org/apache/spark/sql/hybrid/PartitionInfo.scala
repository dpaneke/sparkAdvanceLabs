package org.apache.spark.sql.hybrid

case class PartitionInfo(
  path: String,
  commitMillis: Long
)
