package reader.file

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import reader.Reader

case class LocalFileReader(spark: SparkSession, mainSourcePath: String) extends Reader {

  override def read(folderName: String, fileName: String, schemaResult: StructType): DataFrame =
    spark
      .read
      .schema(schemaResult)
      .parquet(s"$mainSourcePath/$folderName/$fileName")

  def read(folderName: String, fileName: String): DataFrame =
    spark
      .read
      .parquet(s"$mainSourcePath/$folderName/$fileName")
}
