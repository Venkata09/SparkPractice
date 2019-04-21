package api

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by vdokku on 1/29/2018.
  */
object UnionAndIntersectionInRDD_1 {


  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")


    val spark: SparkSession = SparkSession
      .builder().master("local[4]")
      .appName("<<<<<<<<<<<< UnionAndIntersectionInRDD >>>>>>>>>>> ")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext

    val lista = List((1, 2), (2, 3), (4, 5), (6, 7), (8, 9), (0, -1))
    val listb = List((1, 1), (2, 3), (4, 5), (6, 7), (8, 9), (0, 10), (11, 12), (11, 12))
    val rdda = sc.parallelize(lista, 2)
    val rddb = sc.parallelize(listb, 2)

    val rddunion = rdda.union(rddb)
    rddunion.foreach(x => println("union: " + x + " >> "))


    val schema = StructType(
      Seq(
        StructField("fx_marker", StringType, false),
        StructField("timestamp_ms", StringType, false)
      )
    )

    spark.read.option("multiline", value = true).json("C:/Users/vsdokku/Desktop/employee.json")
      .show(2000)

    spark.sqlContext.read
      .option("multiline", value = true).json("C:/Users/vsdokku/Desktop/employee.json")
      .show(2000)



    // ======================== READ STREAM ================================
    println("hello 1")
    // read data stream from Kafka
    val inputStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "streaming1")
      /*.option("startingOffsets", ".kafkaStartingOffset")*/
      .option("maxOffsetsPerTrigger", "100")
      .load()

    println("hello 2")
    //Show Data after processed
    val printConsole = inputStream.writeStream
      .format("console")
      // .option("truncate","false")
      .start()

    println("hello 3")
    printConsole.awaitTermination()



    /*

union: (0,-1) >>
union: (0,10) >>

union: (1,1) >>
union: (1,2) >>

union: (2,3) >>
union: (2,3) >>

union: (8,9) >>
union: (8,9) >>

union: (6,7) >>
union: (6,7) >>

union: (11,12) >>
union: (11,12) >>

union: (4,5) >>
union: (4,5) >>

     */


    val rddintersection = rdda.intersection(rddb)
    rddintersection.foreach(x => println("intersection: " + x + " >>>"))


    val rddDistinct = rddb.distinct()
    rddDistinct.foreach(x => println("distinct: " + x + " >>>"))

    /*

    distinct:(4,5) >>>
distinct:(0,10) >>>
distinct:(1,1) >>>
distinct:(2,3) >>>
distinct:(11,12) >>>
distinct:(8,9) >>>
distinct:(6,7) >>>


     */

  }

}
