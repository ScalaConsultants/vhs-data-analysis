import plotly._
import element._
import layout._
import Plotly._
import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions.{lit, when}

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

    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._

    val df = sparkSession.read.parquet("data-models/output/cluster-data") //spark.read.parquet("/tmp/test/df/1.parquet/")

    //df.collect()....

    println()
    println()

    val data = df.collect().map { r =>
      val i0 = r.fieldIndex("cluster")
      val i1 = r.fieldIndex("partOfDay")
      r.getInt(i0) -> getPartOfDay(r.getInt(i1))
    }.toSeq.groupBy(_._1).map { case (c, list) =>
      c -> list.groupBy(_._2).map{case (p, l) => p -> l.length}
    }

    /*val data2 = df.collect().map { r =>
      val i0 = r.fieldIndex("cluster")
      val i1 = r.fieldIndex("partOfDay")
      r.get(i0).asInstanceOf[Int].toString -> getPartOfDay(r.get(i1).asInstanceOf[Int])
    }.toSeq.groupBy(_._2).map { case (pod, list) =>
      pod -> list.groupBy(_._1).map{case (p, l) => p -> l.length}
    }*/

    /*println(data)

    println()
    println()*/

    df.show(10)

    val histograms = data.map { case (c, map) =>
      val list = map.toSeq

      Histogram()
        .withName(c.toString)
        .withX(list.map(_._1))
        .withY(list.map(_._2))
        .withHistfunc(HistFunc.Sum)
        .withHistnorm(HistNorm.Count)
    }.toSeq

    val lay = Layout().withTitle("Part of Day Playings")
    histograms.plot("plots/plot", lay)

    sparkSession.close()
  }

}
