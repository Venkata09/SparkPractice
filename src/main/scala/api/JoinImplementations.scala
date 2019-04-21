package api

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * Created by vdokku on 1/29/2018.
  */
object JoinImplementations {

  def main(args: Array[String]): Unit = {


    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")


    val spark: SparkSession = SparkSession
      .builder()
      .master("local[4]")
      .appName("<<<<<<<<<<<< ByKeyImplementation>>>>>>>>>>> ")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext


    val lista = List((1, 1), (1, 2), (1, 3))
    val listb = List((1, 4), (1, 5), (2, 10))


    val rdda = sc.parallelize(lista, 2)
    val rddb = sc.parallelize(listb, 1)


    val rddc = sc.makeRDD(lista, 2)


    println("This is RDD A " + rdda.partitions.length)
    println("This is RDD C " + rddc.partitions.length)

    val rddjoin = rdda.join(rddb)
    rddjoin.foreach(x => println("join " + x))

    /*
join (1,(1,4))
join (1,(1,5))
join (1,(2,4))
join (1,(2,5))
join (1,(3,4))
join (1,(3,5))
     */

    val rddcogroupjoin = rdda.cogroup(rddb)
    rddcogroupjoin.foreach(x => println("cogroupjoin " + x))

    /*
cogroupjoin (2,(CompactBuffer(),CompactBuffer(10)))
cogroupjoin (1,(CompactBuffer(1, 2, 3),CompactBuffer(4, 5)))
     */


    val rddcartesian = rdda.cartesian(rddb)
    rddcartesian.foreach(x => println("rddcartesian " + x))


    /*


    This is the Expected Output
    rddcartesian ((1,2),(1,4))
    rddcartesian ((1,2),(1,5))
    rddcartesian ((1,2),(2,10))

    rddcartesian ((1,1),(1,4))
    rddcartesian ((1,1),(1,5))
    rddcartesian ((1,1),(2,10))

    rddcartesian ((1,3),(1,4))
    rddcartesian ((1,3),(1,5))
    rddcartesian ((1,3),(2,10))

3 X 3 ==> This is going to the

rddcartesian ((1,2),(1,4)) rddcartesian ((1,2),(1,5)) rddcartesian ((1,2),(2,10))
rddcartesian ((1,1),(1,4)) rddcartesian ((1,1),(1,5)) rddcartesian ((1,1),(2,10))
rddcartesian ((1,3),(1,4)) rddcartesian ((1,3),(1,5)) rddcartesian ((1,3),(2,10))
     */


    //  cartesian

    /*Computes the cartesian product between two RDDs (i.e. Each item of the first RDD is joined with each item of the second RDD)
  	and returns them as a new RDD. (Warning: Be careful when using this function.! Memory consumption can quickly become an issue!)
  	*/
    val x = sc.parallelize(List(1, 2, 3, 4, 5))
    val y = sc.parallelize(List(6, 7, 8, 9, 10))
    x.cartesian(y).collect.foreach(f => println(f))


    //cogroup
    println("cogroup ---cogroup----cogroup")
    val a = sc.parallelize(List((1, "apple"), (2, "banana"), (3, "orange"), (4, "kiwi")), 2)
    val b = sc.parallelize(List((1, "apple"), (5, "computer"), (1, "laptop"), (1, "desktop"), (4, "iPad")), 2)


    a.cogroup(b).collect.foreach(f => println(f))
    /*
(4,(CompactBuffer(kiwi),CompactBuffer(iPad)))
(2,(CompactBuffer(banana),CompactBuffer()))
(1,(CompactBuffer(apple),CompactBuffer(apple, laptop, desktop)))
(3,(CompactBuffer(orange),CompactBuffer()))
(5,(CompactBuffer(),CompactBuffer(computer)))

******************************* Cogroup is DONE *******************************
     */
    println("******************************* Cogroup is DONE *******************************")

    //subtract 2 RRD's
    val diff = a.subtract(b)
    diff.collect().foreach(f => println(f._2))
    /*
kiwi
orange
banana
collectAsMap ---collectAsMap----collectAsMap
     */

    //collectAsMap
    println("collectAsMap ---collectAsMap----collectAsMap")
    val firstRDD = sc.parallelize(List(1, 2, 1, 3, 8, 3), 1)
    val secondRDD = sc.parallelize(List(5, 6, 5, 7, 9, 12), 1)
    val zippedRDD = firstRDD.zip(secondRDD) // This will create an RDD of T and U ....
    zippedRDD.collectAsMap.foreach(f => println(f))

    /*
        (8,9)
        (2,6)
        (1,5)
        (3,12)
     */

    //combineByKey
    println("combineByKey ---combineByKey----combineByKey")
    val a1 = sc.parallelize(List("dog", "cat", "gnu", "salmon", "rabbit", "turkey", "wolf", "bear", "bee"), 3)
    val b1 = sc.parallelize(List(1, 1, 2, 2, 2, 1, 2, 2, 2), 3)
    val c1 = b1.zip(a1) // ZIP basic gaa ==> 1 <==> 1 mapping chestundhi ...
    val d1 = c1.combineByKey(
      List(_),
      (x: List[String], y: String) => y :: x,
      (x: List[String], y: List[String]) => x ::: y)

    d1.collect.foreach(f => println(f))

    /*

(1,List(cat, dog, turkey))
(2,List(gnu, rabbit, salmon, bee, bear, wolf))

     */

    //filterByRange [Ordered]
    println("filterByRange ---filterByRange----filterByRange")
    val randRDD = sc.parallelize(
      List(
        (2, "cat"),
        (6, "mouse"),
        (7, "cup"),
        (3, "book"),
        (4, "tv"),
        (1, "screen"),
        (5, "heater")), 3)
    val sortedRDD = randRDD.sortByKey()

    sortedRDD.filterByRange(1, 3).collect.foreach(f => println(f))


    sc.stop()

  }

}
