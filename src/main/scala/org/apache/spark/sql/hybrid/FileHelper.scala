package org.apache.spark.sql.hybrid

import java.io.{BufferedWriter, File, FileWriter}

object FileHelper {
  def writeIteratorData(path: String, iterator: Iterator[String]): Unit = {
    ensureParentDirectory(path)
    val buffWriter = new BufferedWriter(new FileWriter(new File(path)))
    iterator.foreach { str =>
      buffWriter.write(str)
      buffWriter.write("\n")
    }
    buffWriter.close()
  }

  private def ensureParentDirectory(path: String): Unit = {
    this.synchronized {
      val dir: File = new File(path)
      val parentDir: File = dir.getParentFile
      if (parentDir != null && !parentDir.exists()) {
        parentDir.mkdirs()
      }
    }
  }
}
