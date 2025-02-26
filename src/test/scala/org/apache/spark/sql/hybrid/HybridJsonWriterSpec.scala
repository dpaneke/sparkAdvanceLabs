package org.apache.spark.sql.hybrid

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

class HybridJsonWriterSpec extends AnyFlatSpec with should.Matchers {

  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[1]")
      .appName("test")
      .getOrCreate()

    "Write" should "write A" in {
      spark
        .range(0, 10, 1, 1)
        .withColumn("id", col("id").cast("int"))
        .write.format("hybrid-json")
        .option("path", "/tmp/foo1.json")
        .option("objectName", "test01")
        .save()
    }

}
