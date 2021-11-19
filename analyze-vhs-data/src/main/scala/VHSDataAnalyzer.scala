import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.internal.Logging

object VHSDataAnalyzer extends Logging {

  def main(args: Array[String]) = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("VHSDataAnalyzer")
      .getOrCreate()

    log.info("read vhs enriched data")
    /*
    read data here
     */


    log.info("segmentation of vhs data")


    /*
      K-means algorithm here
    */


    spark.stop()
  }
}