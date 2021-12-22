import plotly._
import element._
import layout._
import Plotly._
import org.apache.spark.sql.catalyst.dsl.expressions
import org.apache.spark.sql.catalyst.dsl.expressions.DslExpression
import org.apache.spark.sql.{Column, SparkSession, functions}
import org.apache.spark.sql.functions.{col, column, count, countDistinct, lit, when}
import plotly.element.ScatterMode.Markers

object Main {

  def getPartOfDay(partOfDay: Int): String = {
    partOfDay match {
      case 0 => "morning"
      case 1 => "afternoon"
      case 2 => "evening"
      case 3 => "night"
    }
  }

  def main(args: Array[String]): Unit = {

    implicit val session = SparkSession.builder().getOrCreate()
    import session.implicits._

    val df = session.read.parquet("data-models/output/cluster-data")

    /*println(data)

    println()
    println()*/

    df.show(10)

    /*val n = df.groupBy("cluster", "partOfDay").agg(
      functions.count(col("userId")) as "numRecords"
    ).show(50)

    println(s"\n\nn: ${n}\n\n")*/

    /*val data = df.collect().map { r =>
      val i0 = r.fieldIndex("cluster")
      val i1 = r.fieldIndex("partOfDay")
      r.getInt(i0).toString -> getPartOfDay(r.getInt(i1))
    }.toSeq.groupBy(_._2).map{ case (pod, list) =>
      pod -> list.groupBy(_._1).map{case (k, v) => k -> v.length}
    }*/

   /* val data = df.groupBy("cluster", "partOfDay").agg(countDistinct("userId" ) as "count")
    data.show()

    val bars = data.collect().map { r =>
      val i0 = r.fieldIndex("cluster")
      val i1 = r.fieldIndex("partOfDay")
      val i2 = r.fieldIndex("count")

      val cluster = r.getInt(i0).toString

      val partOfDay = getPartOfDay(r.getInt(i1))
      val count = r.getLong(i2)

      Tuple3(cluster, partOfDay, count)
    }.groupBy(_._2).map { case (c, list) =>
      Bar(list.map(_._1).toSeq, list.map(_._3).toSeq).withName(c)
    }.toSeq

    val lay = Layout().withTitle("Number playings by user in each cluster partitioned by part of day over 4 months")
    bars.plot("plots/playings.html", lay)*/

    /*val data = df.groupBy("cluster", "partOfDay").sum("numLevelsCompleted")
    data.show()

    val bars = data.collect().map { r =>
      val i0 = r.fieldIndex("cluster")
      val i1 = r.fieldIndex("partOfDay")
      val i2 = r.fieldIndex("sum(numLevelsCompleted)")

      val cluster = r.getInt(i0).toString

      val partOfDay = getPartOfDay(r.getInt(i1))
      val count = r.getLong(i2)

      Tuple3(cluster, partOfDay, count)
    }.groupBy(_._2).map { case (c, list) =>
      Bar(list.map(_._1).toSeq, list.map(_._3).toSeq).withName(c)
    }.toSeq

    val lay = Layout().withTitle("Number levels users completed in each cluster partitioned by part of day over 4 months")
    bars.plot("plots/levels.html", lay)*/

    /*val bars = Queries.numberOfAdsWatchedByCluster(df)
      .groupBy(_._2).map { case (c, list) =>
      Bar(list.map(_._1).toSeq, list.map(_._3).toSeq).withName(c)
    }.toSeq

    val lay = Layout().withTitle("Number of ads users watched in each cluster partitioned by part of day over 4 months")
    bars.plot("plots/ads.html", lay)*/

    val bars = Queries.organicAdsByCluster(df)
      .groupBy(_._2).map { case (c, list) =>
      Bar(list.map(_._1).toSeq, list.map(_._3).toSeq).withName(c)
    }.toSeq

    val lay = Layout().withTitle("Organic ad views by cluster over 4 months")
    bars.plot("plots/organicAds.html", lay)


    session.close()
  }

}
