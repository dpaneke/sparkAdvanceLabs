package org.apache.spark.sql.hybrid

import org.apache.spark.Partition

case class HybridJsonPartition(
  index: Int,
  path: String,
  commitMillis: Long
) extends Partition
