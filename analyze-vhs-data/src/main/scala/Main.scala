import org.apache.spark.sql.SparkSession
import plotly._, element._, layout._, Plotly._

object Main {

  def main(args: Array[String]): Unit = {

    implicit val session = SparkSession.builder().getOrCreate()
    import session.implicits._

    val df = session.read.parquet("data-models/output/cluster-data")
    //df.show(10)

    val playingsBar = Queries.playersByCluster(df)
      .groupBy(_._2).map { case (c, list) =>
      Bar(list.map(_._1).toSeq, list.map(_._3).toSeq).withName(c)
    }.toSeq

    val playingsLay = Layout().withBarmode(BarMode.Group).withTitle("Active Players by user per cluster")
    playingsBar.plot("plots/activePlayersByCluster.html", playingsLay, useCdn = true,
      openInBrowser = false,
      addSuffixIfExists = true)

    val levelsBar = Queries.levelsCompletedByCluster(df).groupBy(_._2).map { case (c, list) =>
      Bar(list.map(_._1).toSeq, list.map(_._3).toSeq).withName(c)
    }.toSeq

    val levelsLay = Layout().withBarmode(BarMode.Group).withTitle("Levels completed by User per cluster")
    levelsBar.plot(path = "plots/levelCompletedByCluster.html",
      levelsLay,
      useCdn = true,
      openInBrowser = false,
      addSuffixIfExists = true)

    val adsBars = Queries.numberOfAdsWatchedByCluster(df).groupBy(_._2).map { case (c, list) =>
      Bar(list.map(_._1).toSeq, list.map(_._3).toSeq).withName(c)
    }.toSeq

    val adsLay = Layout().withBarmode(BarMode.Group).withTitle("Number of watched ads by cluster")
    adsBars.plot("plots/adsWatchedByCluster.html", adsLay, useCdn = true,
      openInBrowser = false,
      addSuffixIfExists = true)

    val organicBars = Queries.organicAdsByCluster(df).groupBy(_._2).map { case (c, list) =>
      Bar(list.map(_._1).toSeq, list.map(_._3).toSeq).withName(c)
    }.toSeq
    val organicLay =  Layout().withBarmode(BarMode.Group).withTitle("Organic users by cluster")
    organicBars.plot("plots/organicUsersByCluster.html", organicLay, useCdn = true,
      openInBrowser = false,
      addSuffixIfExists = true)

    session.close()
  }

}
