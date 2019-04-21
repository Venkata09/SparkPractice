package data_analysis_on_sample_data.scalaeg

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object ColumnSubsetExample {


  def main(args: Array[String]): Unit = {

    val spark =
      SparkSession.builder().master("local").appName("test").getOrCreate()

    import spark.implicits._

    val data = spark.sparkContext.parallelize(
      Seq(
        (1, Array("Adventure", "Comedy"), Array("Adventure")),
        (2, Array("Animation", "Drama", "War"), Array("War", "Drama")),
        (3, Array("Adventure", "Drama"), Array("Drama", "War"))
      )).toDF("movieId1", "genreList1", "genreList2")

    data.show


    val sampleList1 = List(1, 2, 4, 3, 65, 74)
    val sampleList2 = List(32, 54, 64, 632, 63, 24)

    val sampleList3 = sampleList1 ::: sampleList2

    val someInt = sampleList3 match {
      case Nil => 0
      case n :: rest => n + 20
    }

    val someSetOfFruits = "Apples" :: "Banana" :: "PinaApple" :: "SomeFruits" :: Nil // This is creating a list of
    // strings. Here Strings ni combine chesi, lists chestunnaru.

    /*
    x ::: y ::: z is faster than x ++ y ++ z, because ::: is right associative. x ::: y ::: z is
    parsed as x ::: (y ::: z), which is algorithmically faster than (x ::: y) ::: z (the latter requires O(|x|) more steps).
    Type safety

    With ::: you can only concatenate two Lists. With ++ you can append any collection to List, which is terrible:

     */

    val someSetOfFruits_1 = List("Apples") ::: List("Banana") ::: List("PinaApple") ::: List("SomeFruits") ::: Nil //
    // this is creating a list of list's --> Basic gaa Lists ni combine chestunnaru


    println(" Some Fruits :>>>>>>> " + someSetOfFruits.length)

    println("Some Fruits 1 :>>>>>>> " + someSetOfFruits_1.length)


    println("<<<<<<<< someInt >>>>>> " + someInt)


    println("+++++++++++++++++++++++++++++++++++++++++++")
    sampleList3.foreach(entry => print(entry + ", "))
    println()
    println("+++++++++++++++++++++++++++++++++++++++++++")

    val subsetOf = udf(
      (col1: Seq[String], col2: Seq[String]) // --> Whatever the params that you are trying to pass.
      => {
        if (col2.toSet.subsetOf(col1.toSet)) 1 else 0
      })

    data.withColumn("flag", subsetOf(data("genreList1"), data("genreList2"))).show()


  }


}
