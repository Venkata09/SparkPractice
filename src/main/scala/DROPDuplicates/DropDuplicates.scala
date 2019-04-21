package DROPDuplicates

import org.apache.spark.sql.SparkSession

object DropDuplicates {

  def main(args: Array[String]): Unit = {


    val sparkSession = SparkSession.builder().master("local[4]")
      .appName("<<<< Aggregate by key test >>>>>")
      .getOrCreate()

    val sc = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext

    import sparkSession.implicits._

    val data = sc.parallelize(List(("Foo", 41, "US", 3),
      ("Foo", 39, "UK", 1),
      ("Bar", 57, "CA", 2),
      ("Bar", 72, "CA", 2),
      ("Baz", 22, "US", 6),
      ("Baz", 36, "US", 6))).toDF("x", "y", "z", "count")

    /*
    +---+---+---+-----+
|  x|  y|  z|count|
+---+---+---+-----+
|Foo| 41| US|    3|
|Foo| 39| UK|    1|
|Bar| 57| CA|    2|
|Bar| 72| CA|    2|
|Baz| 22| US|    6|
|Baz| 36| US|    6|
+---+---+---+-----+
     */

    data.show()


    data.dropDuplicates(Array("x", "count")).show()

    data.dropDuplicates("","").show()

    /*
    +---+---+---+-----+
|  x|  y|  z|count|
+---+---+---+-----+
|Baz| 22| US|    6|
|Foo| 39| UK|    1|
|Bar| 57| CA|    2|
|Foo| 41| US|    3|
+---+---+---+-----+
     */

  }

}
