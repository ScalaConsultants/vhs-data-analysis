package reader

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

trait Reader {
  def read(groupItems: String, item: String, schemaResult: StructType): DataFrame
}