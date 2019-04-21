package api

import org.apache.spark.sql.SparkSession

/**
  * @author vdokku
  */
object UdfForAddingArrayColumns {

  def main(args: Array[String]): Unit = {


    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")


    val spark: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName("Remove-String-From-One-DF-From-Another").getOrCreate()


    import spark.implicits._
    import org.apache.spark.sql.functions.{min, max}
    import org.apache.spark.sql.Row

    import org.apache.spark.sql.functions._


    val inputArray = Array(Array("a:25", "a:30", "b:30"), Array("a:25", "a:30", "b:30"))

    inputArray.map(entry => entry)

    val resultantArray = inputArray.flatMap(entry =>
      entry.map(entry1 => {
        val splittedArray = entry1.split(":")
        (splittedArray(0).trim, splittedArray(1).trim.toInt)
      })
    ).groupBy(_._1).map(entry => (entry._1, entry._2.map(_._2).sum))


    val total = resultantArray.values.sum

    resultantArray.map(entry => (entry._1, entry._2.toDouble / total)).foreach(println)

    println(resultantArray)


    println("==================================================")


    val a = Array(Array("a:25", "a:30", "b:30"), Array("a:25", "a:30", "b:30"))
    val res = a.flatMap(a => a.map(x => {
      val y = x.split(":")
      (y.head, y.last.toInt)
    }))
      .groupBy { case (k, _) => k }
      .map { case (k, vs) => (k, vs.foldLeft(0) { case (t, (_, v)) => t + v }) }
    val tot = res.values.sum
    res.map { case (k, v) => s"$k:$v/$tot" }.toArray




  }


}
