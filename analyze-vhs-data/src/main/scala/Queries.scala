import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object Queries {

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

      val partOfDay = r.getString(i1)
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

      val partOfDay = r.getString(i1)
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

}
