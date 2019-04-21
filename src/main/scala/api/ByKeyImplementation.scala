package api

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * Created by vdokku on 1/29/2018.
  */
object ByKeyImplementation {


  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")


    val spark: SparkSession = SparkSession.builder().master("local[4]").appName("<<<<<<<<<<<< ByKeyImplementation>>>>>>>>>>> ").getOrCreate()
    val sc: SparkContext = spark.sparkContext

    val sampleList = List((1, 1), (1, 2), (1, 3), (1, 4), (2, 6))

    val rddmap = sc.parallelize(sampleList, 3)

    val reduceByKeyRDD = rddmap.reduceByKey((x, y) => x + y)
    /*
         In this case X is an integer and that is the value.... based on the above column.
    */

    println("**************************************************")
    reduceByKeyRDD.collect().foreach(entry => println(entry))
    println("**************************************************")


    val rddgroupbykey = rddmap.groupByKey()

    rddgroupbykey.foreach(entry => {
      val thisisIter = entry._2.iterator
      var countVal = 0

      while (thisisIter.hasNext) {
        countVal = countVal + thisisIter.next()
      }
    })


    rddgroupbykey.foreach(entry => println(entry._1 + " === " + entry._2))
    rddgroupbykey.foreach(entry => {
      val it = entry._2.iterator
      var c = 0

      while (it.hasNext) {
        c = c + it.next()
        //        println(" Total sum for this Key:>  " + entry._1 + "  IS:>>> " + c)
      }
      println(" Total sum for this Key:>  " + entry._1 + "  IS:>>> " + c)

    })


    val rddSortByKey = rddmap.sortByKey(true)
    rddSortByKey.foreach(entry => println(" Entry 1:>>>> " + entry._1 + " -- Entry 2:>>>> " + entry._2))


    val rddReduceByKey = rddmap.reduceByKey((v1, v2) => v1 - v2)
    rddReduceByKey.foreach(x => println("reducebykey " + x._1 + " :" + x._2))

    //aggregateByKey(zeroValue)(seqOp, combOp, [numTasks])
    //seqOp : This is Execute in each partition
    //combOp: Executed between different partitions


    val aggregatebykey = rddmap.aggregateByKey("100")(((u, v) => {
      println("FIRST Value:- " + u + " SECOND Value:- " + v);
      u + v
    }), ((v1, v2) => {
      println("FIRST Value:- " + v1 + " SECOND Value:- " + v2);
      v1 + v2
    }))
    aggregatebykey.foreach((x) => println("aggregatebykey:" + x))


    rddmap.aggregateByKey("123")(((u, va) => {
      println("FIRST Value:- " + u + " SECOND Value:- " + va);
      u + va
    }), ((v1, v2) => {
      println("FIRST Value:- " + v1 + " SECOND Value:- " + v2);
      v1 + v2
    }))

    val pairRDD = sc.parallelize(
      List(("cat", 2),
        ("cat", 5),
        ("mouse", 4),
        ("cat", 12),
        ("dog", 12),
        ("mouse", 2)), 2)

    //lets have a look at what is in the partitions
    pairRDD.mapPartitionsWithIndex(myfunc).collect.foreach(f => println(f))


    pairRDD.aggregateByKey(0)(math.max(_, _), _ + _).collect.foreach(f => println(f))
    println("-----------------------------------------------")

    pairRDD.aggregateByKey(100)(math.max(_, _), _ + _).collect.foreach(f => println(f))


    val words = Array("a", "b", "b", "c", "d", "e", "a", "b", "b", "c", "d", "e", "b", "b", "c", "d", "e")
    val wordPairsRDD = sc.parallelize(words).map(word => (word, 1))


    val wordCountsWithReduce = wordPairsRDD
      .reduceByKey(_ + _)
    /*.collect()*/

    /*

    (4) ShuffledRDD[15] at reduceByKey at ByKeyImplementation.scala:106 []
 +-(4) MapPartitionsRDD[14] at map at ByKeyImplementation.scala:102 []
    |  ParallelCollectionRDD[13] at parallelize at ByKeyImplementation.scala:102 []

     */

    println("--------------HELLO ...........----------------")
    println(wordCountsWithReduce.toDebugString)
    println("--------------HELLO >>>>>>>>>>>>----------------")
    wordCountsWithReduce.foreach(f => println(f))
    /*
    (d,3)
    (e,3)
    (a,2)
    (b,6)
    (c,3)
     */

    //Avoid GroupByKey
    println("Avoid GroupByKey")
    val wordCountsWithGroup = wordPairsRDD
      .groupByKey()
      .map(t => (t._1, t._2.sum))
      .collect()
    wordCountsWithGroup.foreach(f => println(f))

    val sampleWordsWithGroup = wordPairsRDD.groupByKey()
      .map(entry => (entry._1, entry._2.sum)).collect()

    /*

     */

    /*
    (d,3)
(e,3)
(a,2)
(b,6)
(c,3)
     */

  }

  def myfunc(index: Int, iter: Iterator[(String, Int)]): Iterator[String] = {
    iter.toList.map(x => "[partID:" + index + ", val: " + x + "]").iterator
  }


}
