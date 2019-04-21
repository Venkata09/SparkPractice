package feedback

/**
  *
  * ACID Properties.
  *
  *
  *
  * https://cnawan.wordpress.com/2016/10/29/comparison-of-aggregatebykey-combinebykey-groupbykey-reducebykey-of-apache-spark/
  * Created by vdokku on 6/28/2017.
  */
object SampleClass {

  def main(args: Array[String]): Unit = {


    import org.apache.spark.sql.SparkSession
    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")

    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("Time Usage")
        .config("spark.master", "local")
        .getOrCreate()

    // For implicit conversions like converting RDDs to DataFrames

    val rawRDD = spark.sparkContext.textFile("C:\\Venkat_Downloads\\Map_of_Police_Department_Incidents.csv")

    case class PoliceData(IncidntNum: String, Category: String, Descript: String, DayOfWeek: String, Date: String, Time: String, PdDistrict: String,
                          Resolution: String, Address: String, X: String, Y: String, Location: String)

    //This is how you eliminate
    val inputMapPartitionRDD = rawRDD.mapPartitionsWithIndex((intWorks, iterWorks) => if (intWorks == 0) iterWorks.drop(1) else iterWorks)
    val actualDataRDD = inputMapPartitionRDD.map(entry => {
      val entrySplitter = entry.split(",")
      PoliceData(entrySplitter(0), entrySplitter(1), entrySplitter(2), entrySplitter(3), entrySplitter(4), entrySplitter(5), entrySplitter(6), entrySplitter(7), entrySplitter(8), entrySplitter(9), entrySplitter(10), entrySplitter(11))
    })

    /*

    AggregateByKey Operation

    val top5CateogriesOfIncidents = inputRDD.map(_.split(",")).map(x=>(x(1),1))
    .aggregateByKey(0)(((x,y)=>x+y),((x,y)=>x+y))
    .takeOrdered(5)(Ordering[Int].reverse.on(x=>x._2))

     CombineByKey Operation

     val top5CateogriesOfIncidents = inputRDD.map(_.split(",")).map(x=>(x(1),1)) .combineByKey((value=>(value+value)),((intraPartitionAcc:Int,v)=>(intraPartitionAcc+v)),((interPartitionAcc1:Int,interPartitionAcc2:Int)=>(interPartitionAcc1+interPartitionAcc2)))
            .takeOrdered(5)(Ordering[Int].reverse.on(x=>x._2))



     */


    /*

    Now we want the top5 categories of the CRIMES,
    So I am doing the aggrwgate of the categories, SO I will get the entry count for all the categories.


      val top5CateogriesOfIncidents = inputRDD.map(_.split(",")).map(x=>(x(1),1)) .
              combineByKey((value=>(value+value)),((intraPartitionAcc:Int,v)=>(intraPartitionAcc+v)),((interPartitionAcc1:Int,interPartitionAcc2:Int)=>(interPartitionAcc1+interPartitionAcc2)))
      .takeOrdered(5)(Ordering[Int].reverse.on(x=>x._2))




val top5CateogriesOfIncidents = inputRDD.map(_.split(",")).map(x=>(x(1),1))
.groupByKey.map(x=>(x._1,x._2.sum))
.takeOrdered(5)(Ordering[Int].reverse.on(x=>x._2))



val top5CateogriesOfIncidents = inputRDD.map(_.split(",")).map(x=>(x(1),1))
.reduceByKey((x,y)=>x+y)
.takeOrdered(5)(Ordering[Int].reverse.on(x=>x._2))
top5CateogriesOfIncidents.foreach(println)

     */

    // Take ordered vs SortByKey ->


    val aggregatedRDD = actualDataRDD.map(pData => (pData.Category, 1)).
      aggregateByKey(0)((x, y) => x + y, (x, y) => x + y).takeOrdered(5)(Ordering[Int].reverse.on(x => x._2))

    val combineByKeyRDD = actualDataRDD.map(pData => (pData.Category, 1)).combineByKey(
      value => value + value,
      (intraPartitionAcc: Int, v) => intraPartitionAcc + v,
      (intraPartitionAcc1: Int, intraPartitionAcc2: Int) => intraPartitionAcc1 + intraPartitionAcc2
    ).takeOrdered(5)(Ordering[Int].reverse.on(x => x._2))


    val groupByKeyRDD = actualDataRDD.map(pData => (pData.Category, 1)).groupByKey.map(rec => (rec._1, rec._2.sum)).takeOrdered(5)(Ordering[Int].reverse.on(x => x._2))


    val reduceByKeyRDD = actualDataRDD.map(pData => (pData.Category, 1)).reduceByKey((x, y) => x + y).takeOrdered(5)(Ordering[Int].reverse.on(x => x._2))


    val aggregatedByKeyExample = actualDataRDD.map(pData => (pData.Category, 1))
      .aggregateByKey(0)(((x, y) => x + y), (x, y) => x + y).takeOrdered(5)(Ordering[Int].reverse.on(x => x._2))

    val reduceByKeyExample = actualDataRDD.map(pData => (pData, 1)).reduceByKey((x, y) => x + y).takeOrdered(6)(Ordering[Int].reverse.on(x => x._2))

    val combineByKeyExample = actualDataRDD.map(pData => (pData, 1)).combineByKey(value => value + value,
      (intra1: Int, v) => intra1 + v,
      (intra1: Int, intra2: Int) => intra1 + intra2)





      /*

      Spark memory -> Spark.memory.fraction -> 0.75 & User memory -> 0.25

      Out of the Spark Memory, there are two sections -> ( Storage memory & Execution memory) spark.memory.storageFraction & spark.memory.

     */
  }

}
