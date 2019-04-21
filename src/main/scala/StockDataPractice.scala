import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by vdokku on 1/3/2018.
  */
object StockDataPractice {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")


    val sparkSession = SparkSession.builder().master("local[4]")
      .appName("<<<< StockData Practice - Select EXPR >>>>>")
      .getOrCreate()

    val sc = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext


    val stocksDataFrame = sparkSession.read.option("header", "true").csv("src/main/resources/datamantra/applestock.csv")


    stocksDataFrame.show()

    val stocks2016 = stocksDataFrame.filter("year(Date)==2016")
    stocks2016.show()


    import org.apache.spark.sql.functions._


    // Tumble is to FALL DOWN
    val tumblingWindowDataSet = stocks2016.groupBy(window(stocks2016.col("Date"), "1 week"))
      .agg(avg("Close")
        .as("weekly_average"))

    println("<<<<<<<<<<<<<<< Weekly average in 2016 using tumbling window >>>>>>>>>>>>>> ")
    printWindow(tumblingWindowDataSet, "weekly_average")

    val windowWithStartTime = stocks2016.groupBy(window(stocks2016.col("Date"), "1 week", "1 week", "4 days")).
      agg(avg("Close").as("weekly_average"))
    println("weekly average in 2016 using sliding window is")
    printWindow(windowWithStartTime, "weekly_average")

    val filteredWindow = windowWithStartTime.filter("year(window.start)=2016")
    println("weekly average in 2016 after filtering is")
    printWindow(filteredWindow, "weekly_average")


  }

  def printWindow(windowDF: DataFrame, aggCol: String) = {
    windowDF.sort("window.start").select("window.start", "window.end", s"$aggCol").
      show(truncate = false)
  }
}
