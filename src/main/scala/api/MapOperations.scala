package api

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

/**
  * Created by vdokku on 1/29/2018.
  */
object MapOperations {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")


    val spark: SparkSession = SparkSession.builder().master("local[4]").appName("<<<<<<<<<<<< ByKeyImplementation>>>>>>>>>>> ").getOrCreate()
    val sc: SparkContext = spark.sparkContext

    val list = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 0)
    val rdd = sc.parallelize(list, 1)

    //   map    :   transformations
    //   map(func) 	Return a new distributed dataset formed by passing each element of
    //    the source through a function func.
    val rddmap = rdd.map(_ + 10)
    rddmap.foreach(println)



    /*
    11
12
13
14
15
16
17
18
19
10
     */


    //  flatMap   :    Transformations
    //    flatMap(func) 	Similar to map, but each input item can be mapped to 0 or more output items
    //(so func should return a Seq rather than a single item).
    val rddflatmap = rdd.flatMap(x => for (y <- 1 to x) yield y)
    println("count=" + rddflatmap.count())
    rddflatmap.foreach(x => println("rddflatmap=" + x))

    /*
    rddflatmap=1
rddflatmap=1
rddflatmap=2
rddflatmap=1
rddflatmap=2
rddflatmap=3
rddflatmap=1
rddflatmap=2
rddflatmap=3
rddflatmap=4
rddflatmap=1
rddflatmap=2
rddflatmap=3
rddflatmap=4
rddflatmap=5
rddflatmap=1
rddflatmap=2
rddflatmap=3
rddflatmap=4
rddflatmap=5
rddflatmap=6
rddflatmap=1
rddflatmap=2
rddflatmap=3
rddflatmap=4
rddflatmap=5
rddflatmap=6
rddflatmap=7
rddflatmap=1
rddflatmap=2
rddflatmap=3
rddflatmap=4
rddflatmap=5
rddflatmap=6
rddflatmap=7
rddflatmap=8
rddflatmap=1
rddflatmap=2
rddflatmap=3
rddflatmap=4
rddflatmap=5
rddflatmap=6
rddflatmap=7
rddflatmap=8
rddflatmap=9
     */


    //mapPartitions  : Transformations
    //mapPartitions(func) 	Similar to map, but runs separately on each partition (block) of the RDD,
    //so func must be of type Iterator<T> => Iterator<U> when running on an RDD of type T.
    val rddmappartitions = rdd.mapPartitions(iter => {
      val s = new ArrayBuffer[Int]();

      while (iter.hasNext) {
        val m = iter.next()
        for (i <- 1 to m) s += i
      }
      s.iterator
    }, false)
    rddmappartitions.foreach(println)


    //mapPartitionsWithIndex  : Transformations
    //mapPartitionsWithIndex(func) 	Similar to mapPartitions, but also provides func with an integer value
    //representing the index of the partition, so func must be of type (Int, Iterator<T>) => Iterator<U>
    //when running on an RDD of type T.

    val mapPartitionsWithIndex = rdd.mapPartitionsWithIndex((x, y) => {
      println("partition id is " + x);
      if (x == 0) {
        for (t <- y) yield t + 10
      }; else for (t <- y) yield 0;
    }, false);

    mapPartitionsWithIndex.foreach(println)


    /*
    partition id is 0
              18/01/29 11:51:26 INFO SparkContext: Invoking stop() from shutdown hook
                  11
                  12
                  13
                  14
                  15
                  16
                  17
                  18
                  19
                  10
     */


    val pipedList = List("ab cd efg", "hi jk bx")
    val pipedRdd = sc.parallelize(pipedList, 2)

    pipedRdd.pipe("echo 10").foreach(println)

  }

}
