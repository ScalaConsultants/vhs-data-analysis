package utils

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import scala.reflect.runtime.universe.TypeTag

trait SchemaGenerator {
  def generateSchema: StructType

  protected def generateSchemaFrom[T <: Product : TypeTag]: StructType = ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]
}
