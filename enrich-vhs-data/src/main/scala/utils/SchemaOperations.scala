package utils

import org.apache.spark.sql.types.{StructField, StructType}

object SchemaOperations {
  def mergeSchemas(schemas: List[StructType]): StructType = {
    val fieldsRaw = schemas.flatMap(_.fields.map( field => (field.name, field.dataType))).distinct
    val fieldsMerged = fieldsRaw.map{case (name, dataType) => StructField(name,dataType)}
    StructType(fieldsMerged.toArray)
  }
}
