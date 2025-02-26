package org.apache.spark.sql.hybrid

object WriterMetaConfig {
  val mongoDbName: String = "index_store"
  val fileCollectionName: String = "file_index"
  val schemaCollectionName: String = "schema_index"
  val objectName: Map[String, String] => String = parameters => parameters.getOrElse(
    "objectName",
    throw new IllegalArgumentException("Must specify objectName")
  )
  val filesPath: Map[String, String] => String = parameters => parameters.getOrElse(
    "path",
    throw new IllegalArgumentException("Must specify path")
  )
  val mongoUri: String = sys.env.getOrElse(
    "MONGO_URI",
    throw new IllegalArgumentException("Must specify MONGO_URI")
  )
}
