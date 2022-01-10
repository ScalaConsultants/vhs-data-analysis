package utils

import org.apache.spark.sql.DataFrame
import plotly.Plotly._
import plotly._
import plotly.element.{Color, Marker}
import plotly.layout._


object KMeansPlotter {

  def generatePlots(kmeansResultDf: DataFrame): Unit = {

    val playingsBar = KmeansResultQueries.playersByCluster(kmeansResultDf)
      .groupBy(_._2).map { case (c, list) =>
      Bar(list.map(_._1).toSeq, list.map(_._3).toSeq).withName(c)
    }.toSeq

    val playingsLay = Layout()
      .withBarmode(BarMode.Group)
      .withTitle("Active Players by user per cluster")
    playingsBar.plot("plots/kmeans/activePlayersByCluster.html", playingsLay, useCdn = true,
      openInBrowser = false,
      addSuffixIfExists = true)

    val levelsBar = KmeansResultQueries.levelsCompletedByCluster(kmeansResultDf).groupBy(_._2).map { case (c, list) =>
      Bar(list.map(_._1).toSeq, list.map(_._3).toSeq).withName(c)
    }.toSeq

    val levelsLay = Layout().withBarmode(BarMode.Group).withTitle("Levels completed by User per cluster")
    levelsBar.plot(path = "plots/kmeans/levelCompletedByCluster.html",
      levelsLay,
      useCdn = true,
      openInBrowser = false,
      addSuffixIfExists = true)

    val adsBars = KmeansResultQueries.numberOfAdsWatchedByCluster(kmeansResultDf).groupBy(_._2).map { case (c, list) =>
      Bar(list.map(_._1).toSeq, list.map(_._3).toSeq).withName(c)
    }.toSeq

    val adsLay = Layout().withBarmode(BarMode.Group).withTitle("Number of watched ads by cluster")
    adsBars.plot("plots/kmeans/adsWatchedByCluster.html", adsLay, useCdn = true,
      openInBrowser = false,
      addSuffixIfExists = true)

    val organicBars = KmeansResultQueries.organicUserByCluster(kmeansResultDf).groupBy(_._2).map { case (c, list) =>
      Bar(list.map(_._1).toSeq, list.map(_._3).toSeq).withName(c)
    }.toSeq
    val organicLay = Layout().withBarmode(BarMode.Group).withTitle("Organic users by cluster")
    organicBars.plot("plots/kmeans/organicUsersByCluster.html", organicLay, useCdn = true,
      openInBrowser = false,
      addSuffixIfExists = true)

  }

}
