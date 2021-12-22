import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Queries {

  def getPartOfDay(partOfDay: Int): String = {
    partOfDay match {
      case 0 => "morning"
      case 1 => "afternoon"
      case 2 => "evening"
      case 3 => "night"
    }
  }

  def getOrganic(flag: Int): String = {
    flag match {
      case 0 => "not organic"
      case 1 => "organic"
    }
  }

  def playingsByCluster(df: DataFrame): Seq[Tuple3[String, String, Long]] = {
    df.groupBy("cluster", "partOfDay").agg(countDistinct("userId" ) as "count").collect().map { r =>
      val i0 = r.fieldIndex("cluster")
      val i1 = r.fieldIndex("partOfDay")
      val i2 = r.fieldIndex("count")

      val cluster = r.getInt(i0).toString

      val partOfDay = getPartOfDay(r.getInt(i1))
      val count = r.getLong(i2)

      Tuple3(cluster, partOfDay, count)
    }.toSeq
  }

  def levelsCompletedByCluster(df: DataFrame): Seq[Tuple3[String, String, Long]] = {
    df.groupBy("cluster", "partOfDay").sum("numLevelsCompleted").collect().map { r =>
      val i0 = r.fieldIndex("cluster")
      val i1 = r.fieldIndex("partOfDay")
      val i2 = r.fieldIndex("sum(numLevelsCompleted)")

      val cluster = r.getInt(i0).toString

      val partOfDay = getPartOfDay(r.getInt(i1))
      val count = r.getLong(i2)

      Tuple3(cluster, partOfDay, count)
    }
  }

  def numberOfAdsWatchedByCluster(df: DataFrame): Seq[Tuple3[String, String, Long]] = {
    df.groupBy("cluster", "partOfDay").sum("numAddsWatched").collect().map { r =>
      val i0 = r.fieldIndex("cluster")
      val i1 = r.fieldIndex("partOfDay")
      val i2 = r.fieldIndex("sum(numAddsWatched)")

      val cluster = r.getInt(i0).toString

      val partOfDay = r.getString(i1)
      val count = r.getLong(i2)

      Tuple3(cluster, partOfDay, count)
    }
  }

  def organicAdsByCluster(df: DataFrame): Seq[Tuple3[String, String, Long]] = {
    df.groupBy("cluster", "flagOrganic").agg(countDistinct("userId")).collect().map { r =>
      val i0 = r.fieldIndex("cluster")
      val i1 = r.fieldIndex("flagOrganic")
      val i2 = r.fieldIndex("count(userId)")

      val cluster = r.getInt(i0).toString

      val organic = r.getInt(i1)
      val count = r.getLong(i2)

      Tuple3(cluster, getOrganic(organic), count)
    }
  }

  /*def genericSum[T](df: DataFrame, groupingColumns: Seq[String], sumColumn: String): Seq[Tuple3[String, String, T]] = {
    df.groupBy(groupingColumns.map(col(_)): _*).sum(sumColumn).collect().map { r =>
      val i0 = r.fieldIndex(groupingColumns(0))
      val i1 = r.fieldIndex(groupingColumns(1))
      val i2 = r.fieldIndex(s"sum($sumColumn)")

      val cluster = r.getInt(i0).toString

      val partOfDay = getPartOfDay(r.getInt(i1))
      val count = r.getAs[T](i2)

      Tuple3(cluster, partOfDay, count)
    }
  }*/

}
