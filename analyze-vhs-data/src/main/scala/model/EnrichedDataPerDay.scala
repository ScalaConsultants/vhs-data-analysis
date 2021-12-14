package model

import org.apache.spark.sql.types.StructType
import utils.SchemaGenerator
import java.sql.Date

case class EnrichedDataPerDay(userId: String,
                              gameId: String,
                              flagOrganic: Int,
                              dateRegistration: Date,
                              numLevelsCompleted: Long,
                              numLevelsStarted: Long,
                              numAddsWatched: Long,
                              numAddsIgnored: Long,
                              numAddsProposed: Long,
                              numPurchasesDone: Long,
                              amountPurchasesDoneDol: Double,
                              numPurchasesRejected: Long,
                              amountPurchasesRejectedDol: Double,
                              numPurchasesCanceled: Long,
                              amountPurchasesCanceledDol: Double,
                              partOfDay: String,
                              date: Date
                             )

object EnrichedDataPerDay extends SchemaGenerator {
  override def generateSchema: StructType = generateSchemaFrom[EnrichedDataPerDay]
}