package methods

import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.sql.DataFrame
import plotly._
import plotly.layout._
import plotly.Plotly._

object ElbowMethod {
  final case class KMeansCost(kCluster: Int, costTraining: Double)

  def showClusterCostForElbowMethod(inputKMeansDf: DataFrame, fromK: Int, toK: Int): Unit = {
    val kMeansCosts = for {
      kCluster <- (fromK to toK).toList
      pipelineModel = KMeansMethod.getModelForKMeans(inputKMeansDf, kCluster)
      kmModel = pipelineModel.stages.last.asInstanceOf[KMeansModel]
      kMeansCost = kmModel.summary.trainingCost
    } yield KMeansCost(kCluster, kMeansCost)

    val xCluster = kMeansCosts.map(_.kCluster)
    val yCostTraining = kMeansCosts.map(_.costTraining)
    val plot = Seq(
      Scatter(xCluster, yCostTraining).withName("CostTraining vs k")
    )
    val lay = Layout().withTitle("Elbow method")
    plot.plot(s"plots/elbowFrom${fromK}To${toK}", lay)
  }
}
