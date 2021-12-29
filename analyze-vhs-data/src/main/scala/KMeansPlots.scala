import org.apache.spark.sql.SparkSession
import plotly.Plotly._
import plotly._
import plotly.layout._

object KMeansPlots {

  def main(args: Array[String]): Unit = {

    implicit val session = SparkSession.builder().getOrCreate()

    val df = session.read.parquet("data-models/output/cluster-data")

    val playingsBar = Queries.playersByCluster(df)
      .groupBy(_._2).map { case (c, list) =>
      Bar(list.map(_._1).toSeq, list.map(_._3).toSeq).withName(c)
    }.toSeq

    val playingsLay = Layout().withBarmode(BarMode.Group).withTitle("Active Players by user per cluster")
    playingsBar.plot("plots/kmeans/activePlayersByCluster.html", playingsLay, useCdn = true,
      openInBrowser = false,
      addSuffixIfExists = true)

    val levelsBar = Queries.levelsCompletedByCluster(df).groupBy(_._2).map { case (c, list) =>
      Bar(list.map(_._1).toSeq, list.map(_._3).toSeq).withName(c)
    }.toSeq

    val levelsLay = Layout().withBarmode(BarMode.Group).withTitle("Levels completed by User per cluster")
    levelsBar.plot(path = "plots/kmeans/levelCompletedByCluster.html",
      levelsLay,
      useCdn = true,
      openInBrowser = false,
      addSuffixIfExists = true)

    val adsBars = Queries.numberOfAdsWatchedByCluster(df).groupBy(_._2).map { case (c, list) =>
      Bar(list.map(_._1).toSeq, list.map(_._3).toSeq).withName(c)
    }.toSeq

    val adsLay = Layout().withBarmode(BarMode.Group).withTitle("Number of watched ads by cluster")
    adsBars.plot("plots/kmeans/adsWatchedByCluster.html", adsLay, useCdn = true,
      openInBrowser = false,
      addSuffixIfExists = true)

    val organicBars = Queries.organicAdsByCluster(df).groupBy(_._2).map { case (c, list) =>
      Bar(list.map(_._1).toSeq, list.map(_._3).toSeq).withName(c)
    }.toSeq
    val organicLay =  Layout().withBarmode(BarMode.Group).withTitle("Organic users by cluster")
    organicBars.plot("plots/kmeans/organicUsersByCluster.html", organicLay, useCdn = true,
      openInBrowser = false,
      addSuffixIfExists = true)

    session.close()

  }

}
