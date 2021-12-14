package model

import org.apache.spark.sql.types.StructType
import utils.SchemaGenerator
import java.sql.Date

case class EnrichedDataPerMonth(userId: String,
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
                                codMonth: Int
                               )

object EnrichedDataPerMonth extends SchemaGenerator {
  override def generateSchema: StructType = generateSchemaFrom[EnrichedDataPerMonth]
}