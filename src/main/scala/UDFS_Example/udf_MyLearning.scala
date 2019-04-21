package UDFS_Example


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType


/**
  * @author vdokku
  */
object udf_MyLearning {


  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")
    val sparkSession = SparkSession.builder().master("local[4]")
      .appName("UDFTest ---------------> ")
      .getOrCreate()

    import sparkSession.implicits._
    import org.apache.spark.sql.functions._

    def convertToLowerCaseAndReplaceWhiteSpace = udf(
      (inputString: String) => inputString.toLowerCase().replaceAll("\\s", ""))

    def convertToLowerCaseAndReplaceWhiteSpaceAndHandleNullsToo = udf(
      (inputString: String) => {
        val str = Option.apply(inputString).getOrElse[String]("")
        Some(str.toLowerCase().replaceAll("\\s",""))
      }
    )

    /* For Creating the data set, you can create from the LIST. */
    /* but for creating the data FRAME you need to have an RDD. */
    val sparkCreateDataFrame = sparkSession.createDataset(List("  HI THERE     ", " GivE mE PresenTS     ")).toDF("LIST_VALS")


    println("--------------------------------------------")
    sparkCreateDataFrame.select(convertToLowerCaseAndReplaceWhiteSpace(col("LIST_VALS"))
      .as("Clean_Data")).show()
    println("--------------------------------------------")

    val sparkCreateDataFrame1 = sparkSession.createDataset(List("  HI THERE     ", " GivE mE PresenTS     ", null))
      .toDF("LIST_VALS")


    println("--------------------------------------------")
    sparkCreateDataFrame1.select(convertToLowerCaseAndReplaceWhiteSpaceAndHandleNullsToo(col("LIST_VALS"))
      .as("clean_Data")).show()
    println("--------------------------------------------")


  }
}
