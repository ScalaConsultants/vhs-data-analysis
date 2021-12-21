package model.events

import org.apache.spark.sql.types.StructType
import utils.SchemaGenerator

final case class PurchaseEvent(userId: String,
                               gameId: String,
                               itemId: String,
                               category: String,
                               amount: Double,
                               currency: String,
                               status: String,
                               timestamp: Long,
                               timezone: String
                              )

object PurchaseEvent extends SchemaGenerator {
  override def generateSchema: StructType = generateSchemaFrom[PurchaseEvent]
}

