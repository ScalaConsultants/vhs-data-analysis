package model.events

import org.apache.spark.sql.types.StructType
import utils.SchemaGenerator

final case class LevelEvent(userId: String,
                            gameId: String,
                            levelId: String,
                            levelDifficulty: String,
                            levelProgress: String,
                            status: String,
                            timestamp: Long,
                            timezone: String
                           )

object LevelEvent extends SchemaGenerator {
  override def generateSchema: StructType = generateSchemaFrom[LevelEvent]
}