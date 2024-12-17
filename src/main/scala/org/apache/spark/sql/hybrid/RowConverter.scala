package org.apache.spark.sql.hybrid

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils.TimeZoneUTC
import org.apache.spark.sql.catalyst.expressions.{Literal, StructsToJson}
import org.apache.spark.sql.types.StructType

class RowConverter(dataType: StructType) {
  def toJsonString(input: Iterator[InternalRow]): Iterator[String] = {
    val internalRowConverter: StructsToJson = StructsToJson(
      options = Map.empty,
      child = Literal(null, dataType),
      timeZoneId = Some(TimeZoneUTC.getID)
    )
    input.map(row =>
      internalRowConverter
        .nullSafeEval(row)
        .toString
    )
  }
}
