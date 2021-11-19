package model.events

import org.apache.spark.sql.types.StructType
import utils.SchemaGenerator

final case class AddEvent(userId: String,
                          gameId: String,
                          placementId: String,
                          adType: String,
                          status: String,
                          timestamp: Long
                         )

object AddEvent extends SchemaGenerator {
  override def generateSchema: StructType = generateSchemaFrom[AddEvent]
}
