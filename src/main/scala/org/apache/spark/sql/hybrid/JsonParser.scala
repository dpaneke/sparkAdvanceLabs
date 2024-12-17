package org.apache.spark.sql.hybrid

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.JsonToStructs
import org.apache.spark.sql.catalyst.util.DateTimeUtils.TimeZoneUTC
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

class JsonParser(schema: StructType) {
  def toRow(input: Iterator[String]): Iterator[InternalRow] = {
    val jsonConverter: JsonToStructs = JsonToStructs(
      schema = schema,
      options = Map.empty,
      child = null,
      timeZoneId = Some(TimeZoneUTC.getID)
    )
    input.map(jsonStr =>
      jsonConverter
        .nullSafeEval(UTF8String.fromString(jsonStr))
        .asInstanceOf[InternalRow]
    )
  }
}
