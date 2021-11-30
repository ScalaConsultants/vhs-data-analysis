package reader.mongo

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.sql.types.StructType
import reader.Reader

final case class MongoReader(spark: SparkSession, mongoUri:String) extends Reader {

  override def read(database: String, collection: String, schemaResult: StructType): DataFrame =
  {
    val mongoReadConfig = ReadConfig(Map(
      "uri" -> mongoUri,
      "database" -> database,
      "collection" -> collection
    ))

    MongoSpark.builder().sparkSession(spark).readConfig(mongoReadConfig).build().toDF(schemaResult)
  }
}
