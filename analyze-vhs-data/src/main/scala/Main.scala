import org.apache.spark.sql.SparkSession
import plotly._, element._, layout._, Plotly._

object Main {

  def main(args: Array[String]): Unit = {

    implicit val session = SparkSession.builder().getOrCreate()
    import session.implicits._

    val df = session.read.parquet("data-models/output/cluster-data")
    //df.show(10)

    val playingsBar = Queries.playingsByCluster(df)
      .groupBy(_._2).map { case (c, list) =>
      Bar(list.map(_._1).toSeq, list.map(_._3).toSeq).withName(c)
    }.toSeq

    val playingsLay = Layout().withBarmode(BarMode.Group).withTitle("Playings by user per cluster")
    playingsBar.plot("plots/playingsByCluster.html", playingsLay, useCdn = true,
      openInBrowser = false,
      addSuffixIfExists = true)

    val levelsLay = Layout().withBarmode(BarMode.Group).withTitle("Levels completed by User per cluster")
    val levelsBar = Queries.levelsCompletedByCluster(df).groupBy(_._2).map { case (c, list) =>
      Bar(list.map(_._1).toSeq, list.map(_._3).toSeq).withName(c)
    }.toSeq

    levelsBar.plot(path = "plots/levelsByCluster.html",
      levelsLay,
      useCdn = true,
      openInBrowser = false,
      addSuffixIfExists = true)

<<<<<<< HEAD
    val adsLay = Layout(barmode = BarMode.Group).withTitle("Number of watched ads by cluster")
    val adsBars = Queries.numberOfAdsWatchedByCluster(df).groupBy(_._2).map { case (c, list) =>
      Bar(list.map(_._1).toSeq, list.map(_._3).toSeq).withName(c)
    }.toSeq

    adsBars.plot("plots/adsByCluster.html", adsLay, useCdn = true,
      openInBrowser = false,
      addSuffixIfExists = true)
=======
    val bars1 = Queries.numberOfAdsWatchedByCluster(df)
      .groupBy(_._2).map { case (c, list) =>
      Bar(list.map(_._1).toSeq, list.map(_._3).toSeq).withName(c)
    }.toSeq

    val lay1 = Layout().withTitle("Number of ads users watched in each cluster partitioned by part of day over 4 months")
    bars1.plot("plots/ads.html", lay1)
>>>>>>> f03bbfc9a6f1e4fab81a32aae129f895f7157cfe

    val organicLay =  Layout(barmode = BarMode.Group).withTitle("Organic ads by cluster")
    val organicBars = Queries.organicAdsByCluster(df).groupBy(_._2).map { case (c, list) =>
      Bar(list.map(_._1).toSeq, list.map(_._3).toSeq).withName(c)
    }.toSeq

    organicBars.plot("plots/organicAdsByCluster.html", organicLay, useCdn = true,
      openInBrowser = false,
      addSuffixIfExists = true)

    session.close()
  }

}
