package examples

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}


/**
  * Created by vdokku on 6/18/2017.
  */
object AverageTest {

  def main(args: Array[String]): Unit = {


    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")

    val sparkSession = SparkSession.builder().master("local[4]")
      .appName("my-spark-app")
      .getOrCreate()

    val nums = Seq(("a", 6.0), ("a", 3.0), ("a", 3.0), ("a", 4.0), ("b", 1.1), ("b", 1.1), ("b", 1.1))
    val numsRDD = sparkSession.sparkContext.parallelize(nums, 2)

    val numsRowRDD = numsRDD.map { x => Row(x._1, x._2) }

    val schema = new StructType().add(StructField("id", StringType, nullable = false)).add(StructField("num", DoubleType, nullable = true))
    val numsDF = sparkSession.createDataFrame(numsRowRDD, schema)

    numsDF.createTempView("myTable")

    sparkSession.sql("select id,avg(num) from mytable  group by id").collect()
      .foreach { x => println(s"id:${x(0)},avg:${x(1)}") }

    sparkSession.udf.register("myAvg", Average_UDAF)
    sparkSession.sql("select id,myAvg(num) from mytable group by id ").collect().foreach { x => println(s"id:${x(0)},avg:${x(1)}") }

    sparkSession.stop()

  }
}
