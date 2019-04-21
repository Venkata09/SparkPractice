package data_analysis_on_sample_data.scalaeg2

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}


object ZipTwoRDD {


  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder()
      .master("local")
      .appName("ParquetAppendMode")
      .getOrCreate()

    import spark.implicits._

    val df1 = spark.sparkContext.parallelize(Seq(
      (1, "abc"),
      (2, "def"),
      (3, "hij")
    )).toDF("id", "name")

    val df2 = spark.sparkContext.parallelize(Seq(
      (19, "x"),
      (29, "y"),
      (39, "z")
    )).toDF("age", "address")


    //Scala list concatenation, ::: vs ++

    val schema = StructType(df1.schema.fields ++ df2.schema.fields)

    val df1df2_1 = df1.rdd.zip(df2.rdd)

    println("---------------------------")
    df1df2_1.foreach(entry => println(entry._1.toSeq ++ entry._2.toSeq))
    println("---------------------------")

    println("---------------------------")
    println(schema)
    println("---------------------------")

    val df1df2 = df1.rdd.zip(df2.rdd).map {
      case (rowLeft, rowRight) => Row.fromSeq(rowLeft.toSeq ++ rowRight.toSeq)
    }

    spark.createDataFrame(df1df2, schema).show()


    val d = spark.sparkContext.parallelize(Seq(
      ("ron", 2, 4543),
      ("aky", 3, 5632),
      ("kia", 4, 1432)
    )).toDF("emp", "empId", "salary")


    d.groupBy($"empID").agg(max($"salary").alias("maxSalary"), min($"salary").alias("minSalary"))


    /*


    +---+----+---+-------+

    | id|name|age|address|
    +---+----+---+-------+
    |  1| abc| 19|      x|
      |  2| def| 29|      y|
      |  3| hij| 39|      z|
      +---+----+---+-------+


      */


    val line = spark.sparkContext.parallelize(Array(("2,SMITH,AARON"), ("2,SMITH,AARON")))

    val result = line.map(r => {
      val split = r.split(",")
      (split(0).toInt, split.tail.mkString(", "))
    }).toDF("a", "b").withColumn("c", lit(null))

    result.coalesce(1).rdd.saveAsTextFile("C:\\Venkat\\Spark_ML_Temp\\testfile.csv")


  }


}
