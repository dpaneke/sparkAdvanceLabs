package org.apache.spark.sql.hybrid

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
class JsonParser(schema: StructType) {
  def toRow(input: Iterator[String]): Iterator[InternalRow] = {
    ???
  }
}
