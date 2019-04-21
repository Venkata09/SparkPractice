package api

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * Created by vdokku on 1/29/2018.
  */
object RepartitionImplementations {


  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")


    val spark: SparkSession = SparkSession
      .builder()
      .master("local[4]")
      .appName("<<<<<<<<<<<< RepartitionImplementations >>>>>>>>>>> ")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext


    val list = List(1, 2, 3, 4, 5, 6)
    val rdd = sc.parallelize(list, 1)
    println("before repartition : " + rdd.getNumPartitions)

    val rddrepartition = rdd.repartition(3)
    rddrepartition.foreach { x => print("setp1= " + x + " >>  ") }
    println("after repartition : " + rddrepartition.getNumPartitions)

    /*
    after repartition : 3
    setp1= 1 >>  setp1= 2 >>  setp1= 3 >>  setp1= 5 >>  setp1= 6 >>  setp1= 4 >>
     */

    val rddcoalesce = rddrepartition.coalesce(2, true)
    //    rddcoalesce.saveAsTextFile("/src/main/resources/repartition")
    rddcoalesce.count()
    rddcoalesce.foreach { x => print("setp2= " + x + " >>> ") }
    println("after rddcoalesce : " + rddcoalesce.getNumPartitions)

    /*
    setp2= 3 >>> setp2= 6 >>> setp2= 1 >>> setp2= 4 >>> setp2= 2 >>> setp2= 5
    after rddcoalesce : 2
    */

    val rddcoalescet = rddrepartition.coalesce(4, true)
    rddcoalescet.foreach { x => print("setp3=" + x + " >>> ") }
    println("after rddcoalesce : " + rddcoalescet.getNumPartitions)

      /*
      setp3=6 >>> setp3=4 >>> setp3=5 >>>
       */

  }

}
