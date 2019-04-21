package StackOverflow

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * Created by vdokku on 10/10/2017.
  */
object GroupBy_Aggregate_Functions {

  case class Step(Id: Long,
                  stepNum: Long,
                  stepId: Int,
                  stepTime: java.sql.Timestamp
                 )

  /*  RDD[(Long, Iterable[Step])] = inquiryStepMap.rdd.groupBy(x => x.Id)*/

  def main(args: Array[String]): Unit = {


    val spark: SparkSession = SparkSession.builder().master("local[4]").appName("QueryingWithStreamingSources").getOrCreate()
    val sc: SparkContext = spark.sparkContext
    System.setProperty("hadoop.home.dir", "C:\\Venkata_DO\\hadoop-common-2.2.0-bin-master")

    import java.sql.Timestamp

    import spark.sqlContext.implicits._
    val t = new Timestamp(2017, 5, 1, 0, 0, 0, 0)
    val ds = Seq(Step(1L, 21L, 1, t), Step(1L, 20L, 2, t), Step(2L, 10L, 3, t)).toDS()

    ds.groupByKey(_.Id).mapGroups((id, vals) => (id, vals.toList)).show()

    // SO a groupbyKey will have the following methods.

    /*1) mapValues
    2) mapGroups
    3) flatMapGroups*/


    import org.apache.spark.sql.functions._ //for count()

    import org.apache.spark.sql.functions.udf

    // Defining the UDF.
    val upper: String => String = _.toUpperCase
    val encodeUDF = udf(upper)




    spark.sqlContext.sql("SELECT encoder(colname) from test").show()



    // This might not work, but syntatically no issues.

    ds.cache().withColumn("timePeriod", encodeUDF(col("START_TIME")))
      .groupBy("timePeriod")
      .agg(
        mean("DOWNSTREAM_SIZE").alias("Mean"),
        stddev("DOWNSTREAM_SIZE").alias("Stddev"),
        count(lit(1)).alias("Num Of Records")
      )
      .show(20, false)


    /*timePeriod | Mean | Stddev | Num Of Records
    X      | 10   |   20   |    315*/


  }
}
