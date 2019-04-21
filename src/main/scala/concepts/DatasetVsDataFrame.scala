package concepts

import org.apache.spark.sql.SparkSession

/**
  * Logical Plans & Differences between Dataframe and Dataset
  */
object DatasetVsDataFrame {

  case class Sales(transactionId: Int, customerId: Int, itemId: Int, amountPaid: Double)

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")
    val sparkSession = SparkSession.builder.
      master("local")
      .appName("example")
      .getOrCreate()

    val sparkContext = sparkSession.sparkContext
    import sparkSession.implicits._


    //read data from text file

    println("<<<<<<<<< Data Frame Optimized Plan >>>>>>>>>>>>>>>>>.")
    val df = sparkSession.read.option("header", "true").option("inferSchema", "true").csv("src/main/resources/datamantra/sales.csv")
    println("<<<<<<<<< Data Set Optimized Plan >>>>>>>>>>>>>>>>>.")
    val ds = sparkSession.read.option("header", "true").option("inferSchema", "true").csv("src/main/resources/datamantra/sales.csv").as[Sales]


    val selectedDF = df.select("itemId")

    val selectedDS = ds.map(_.itemId)

    println(selectedDF.queryExecution.optimizedPlan.numberedTreeString)

    println(selectedDS.queryExecution.optimizedPlan.numberedTreeString)


  }

}
