package model.events

import org.apache.spark.sql.types.StructType
import utils.SchemaGenerator

final case class AdEvent(userId: String,
                         gameId: String,
                         placementId: String,
                         adType: String,
                         status: String,
                         timestamp: Long,
                         timezone: String
                         )

object AdEvent extends SchemaGenerator {
  override def generateSchema: StructType = generateSchemaFrom[AdEvent]
}
