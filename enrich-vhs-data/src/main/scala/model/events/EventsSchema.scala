package model.events

import org.apache.spark.sql.types.StructType
import utils.SchemaGenerator

object EventsSchema {
  private val listOfPlayerInfoEvents: List[SchemaGenerator] = List(
    PlayerEvent
  )

  private val listOfPlayerBehaviorEvents: List[SchemaGenerator] = List(
    LevelEvent,
    AddEvent,
    PurchaseEvent
  )

  def getAllPlayerInfoEventsSchema: List[StructType] = listOfPlayerInfoEvents.map(_.generateSchema)

  def getAllPlayerBehaviorEventsSchema: List[StructType] = listOfPlayerBehaviorEvents.map(_.generateSchema)
}
