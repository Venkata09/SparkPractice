package api

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object WholeStagen_Example {

  System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")


  val spark: SparkSession = SparkSession
    .builder()
    .master("local[4]")
    .appName("<<<<<<<<<<<< WHOLE State Code GEN >>>>>>>>>>> ")
    .getOrCreate()

  val sc: SparkContext = spark.sparkContext

  def benchmark(name: String)(f: => Unit) = {
    val startTime = System.nanoTime()
    f
    val endTime = System.nanoTime()
    println(s"$name in ${(endTime - startTime).toDouble / 1000000000.0}s")
  }

  def sumValues() = {
    benchmark("Sum Values") {
      spark.range(1000 * 1000 * 1000).selectExpr("sum(id)").show
    }
  }

  def joinValues() = {
    benchmark("joinValues") {
      val df1 = spark.range(1000 * 1000 * 1000)
      val df2 = spark.range(1000)
      val joined = df1.join(df2, "id")
      joined.explain(true)
      val joinedCount = joined.count()
      println(s"Joined count: $joinedCount")

    }
  }

  def benchmark1() = {
    spark.conf.set("spark.sql.codegen.wholeStage", false)
    println("spark.sql.codegen.wholeStage=false")
    // Sum Values in 14.815269276s
    sumValues()
    spark.conf.set("spark.sql.codegen.wholeStage", true)
    println("spark.sql.codegen.wholeStage=true")
    // Sum Values in 0.877105465s
    sumValues()
  }

  def benchmark2() = {
    spark.conf.set("spark.sql.codegen.wholeStage", false)
    println("spark.sql.codegen.wholeStage=false")
    // joinValues in 30.123704777s
    joinValues()
    spark.conf.set("spark.sql.codegen.wholeStage", true)
    println("spark.sql.codegen.wholeStage=true")
    // joinValues in 1.569773731s
    joinValues()
  }

  def main(args: Array[String]): Unit = {
    benchmark1()
    benchmark2()
  }
}
