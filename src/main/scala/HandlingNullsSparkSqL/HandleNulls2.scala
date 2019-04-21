package HandlingNullsSparkSqL

import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.util.Try

/**
  * Created by vdokku on 2/11/2018.
  */
object HandleNulls2 {


  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")


    val spark: SparkSession = SparkSession.builder()
      .master("local[4]").appName("<<<<<<<<<<<< ByKeyImplementation>>>>>>>>>>> ")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext

    import spark.implicits._


    val data = List(
      List(444.1235D),
      List(67.5335D),
      List(69.5335D),
      List(677.5335D),
      List(47.5335D),
      List(null)
    )

    val rdd = spark.sparkContext.parallelize(data).map(Row.fromSeq(_))

    val finalReturnedValue = spark.sparkContext.parallelize(data).map(_.flatMap(Option(_))).filter(_.nonEmpty)





    println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    println(rdd.foreach(println)) // see this has the NULL values.

    /*
    [444.1235]
[67.5335]
[47.5335]
[677.5335]
[null]
[69.5335]
     */
    println(finalReturnedValue.foreach(println)) // This for removing the null values.

    /*
    List(47.5335)
    List(67.5335)
    List(69.5335)
    List(677.5335)
    List(444.1235)

     */

    println("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")


    val schema = StructType(Array(
      StructField("value", DataTypes.DoubleType, true)
    ))

    val df = spark.sqlContext.createDataFrame(rdd, schema)

    df.select()


    val doubleUdf: UserDefinedFunction = udf((v: Any) => Try(v.toString.toDouble).toOption)


    //    val multip: Dataset[Double] = df.select(doubleUdf(df("value"))).as[Double]


    val multip: Dataset[Double] = df.select(doubleUdf(df("value"))).as[Double]

    val multiply = multip.filter(_ != null).reduce(_ * _)

    // na is used to remove the numm values.


    println("<<<<<<<<<< After multiplication >>>>>>>>>>>>> " + multiply)


  }

}
