import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions

object SparkDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val simpleData = Seq(("James", "Sales", 3000),
      ("Michael", "Sales", 4600),
      ("Robert", "Sales", 4100),
      ("Maria", "Finance", 3000),
      ("James", "Sales", 3000),
      ("Scott", "Finance", 3300),
      ("Jen", "Finance", 3900),
      ("Jeff", "Marketing", 3000),
      ("Kumar", "Marketing", 2000),
      ("Saif", "Sales", 4100)
    )
    val df = simpleData.toDF("employee_name", "department", "salary")

    /*df.select(functions.countDistinct("department", "salary"))
      .show(100)*/

    df.groupBy("department")

    //df.show()

    spark.close()

  }

}
