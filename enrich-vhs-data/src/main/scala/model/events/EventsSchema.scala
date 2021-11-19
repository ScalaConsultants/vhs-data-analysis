package model.events

import org.apache.spark.sql.types.StructType
import utils.SchemaGenerator

object EventsSchema {
  private val listOfEvents: List[SchemaGenerator] = List(
    LevelEvent,
    AddEvent,
    PurchaseEvent
  )

  def getAllEventsSchema: List[StructType] = listOfEvents.map(_.generateSchema)
}
