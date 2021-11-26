package model.events

import org.apache.spark.sql.types.StructType
import utils.SchemaGenerator

final case class PlayerEvent(userId: String,
                             gameId: String,
                             attribution: String,
                             action: String,
                             timestamp: Long
                            )

object PlayerEvent extends SchemaGenerator {
  override def generateSchema: StructType = generateSchemaFrom[PlayerEvent]
}