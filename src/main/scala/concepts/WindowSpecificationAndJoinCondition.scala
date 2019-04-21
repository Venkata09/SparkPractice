package concepts

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType, _}

/**
  * Created by vdokku on 2/22/2018.
  */
object WindowSpecificationAndJoinCondition {

  def main(args: Array[String]): Unit = {


    val sparkSession = SparkSession.builder().appName("DataFrame VS DataSet").master("local[4]")
      .appName("<<<< GroupByExample  >>>>>")
      .getOrCreate()

    val sc = sparkSession.sparkContext


    val get_cus_val = sparkSession.udf.register("get_cus_val",
      (filePath: String) => filePath.split("\\.")(3))
    val get_cus_YearPartition = sparkSession.udf.register("get_cus_YearPartition",
      (filePath: String) => filePath.split("\\.")(4))


    import sparkSession.implicits._

    val rdd1 = sparkSession.sparkContext.textFile("src/main/resources/window_example/orgData.txt")

    val header1 = rdd1.filter(_.contains("OrganizationId")).map(line => line.split("\\|\\^\\|")).first()

    val schema1 = StructType(header1.map(cols => StructField(cols.replace(".", "_"), StringType)).toSeq)

    val data = sparkSession.sqlContext
      .createDataFrame(rdd1.filter(!_.contains("OrganizationId"))
        .map(line => Row.fromSeq(line.split("\\|\\^\\|").toSeq)), schema1)

    val schemaHeader = StructType(header1.map(cols => StructField(cols.replace(".", "."), StringType)).toSeq)
    val dataHeader = sparkSession.sqlContext.createDataFrame(rdd1.filter(!_.contains("OrganizationId")).map(line => Row.fromSeq
    (line.split("\\|\\^\\|").toSeq)), schemaHeader)
    import org.apache.spark.sql.functions.input_file_name


    val df1resultFinal = data.withColumn("DataPartition", get_cus_val(input_file_name))
    val df1resultFinalWithYear = df1resultFinal.withColumn("PartitionYear", get_cus_YearPartition(input_file_name))


    data.show()

    val df2 = sparkSession.sparkContext.textFile("")

    //Loading Incremental

    val rdd2 = sc.textFile("src/main/resources/window_example/orgData2.txt")
    val header2 = rdd2.filter(_.contains("OrganizationId")).map(line => line.split("\\|\\^\\|")).first()
    val schema2 = StructType(header2.map(cols => StructField(cols.replace(".", "_"), StringType)).toSeq)
    val data2 = sparkSession.sqlContext.createDataFrame(rdd2.filter(!_.contains("OrganizationId")).map(line => Row
      .fromSeq(line
        .split("\\|\\^\\|").toSeq)), schema1)


    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions._

    val windowSpec = Window.partitionBy("OrganizationId", "AnnualPeriodId", "InterimPeriodId").orderBy($"TimeStamp".cast(LongType).desc)
    val latestForEachKey1 = data.withColumn("rank", rank().over(windowSpec)).filter($"rank" === 1).drop("rank")


    val windowSpec2 = Window.partitionBy("OrganizationId", "InterimPeriodId").orderBy($"TimeStamp".cast(LongType).desc)
    val latestForEachKey = latestForEachKey1.withColumn("tobefiltered", first("FFAction|!|").over(windowSpec2))
      .filter($"tobefiltered" === "I|!|" || $"tobefiltered" === "O|!|" || ($"tobefiltered" === "D|!|" && $"FFAction|!|" === "D|!|"))
      .drop("tobefiltered", "TimeStamp")


    /*OrganizationId|^|AnnualPeriodId|^|InterimPeriodId|^|InterimNumber|^|FFAction
      4295858898|^|204|^|205|^|1|^|I|!|
      4295858898|^|204|^|208|^|2|^|I|!|
      4295858898|^|204|^|209|^|2|^|I|!|
      4295858898|^|204|^|211|^|3|^|I|!|
      4295858898|^|204|^|212|^|3|^|I|!|
      4295858898|^|204|^|214|^|4|^|I|!|
      4295858898|^|204|^|215|^|4|^|I|!|
      4295858898|^|206|^|207|^|1|^|I|!|
      4295858898|^|206|^|210|^|2|^|I|!|
      4295858898|^|206|^|213|^|3|^|I|!|*/

    /* DataPartition|^|PartitionYear|^|TimeStamp|^|OrganizationId|^|AnnualPeriodId|^|InterimPeriodId|^|InterimNumber|^|FFAction|!|
       SelfSourcedPublic|^|2002|^|1511224917595|^|4295858941|^|24|^|25|^|4|^|O|!|
       SelfSourcedPublic|^|2002|^|1511224917596|^|4295858941|^|24|^|25|^|4|^|O|!|
       SelfSourcedPublic|^|2003|^|1511224917597|^|4295858941|^|30|^|31|^|2|^|O|!|
       SelfSourcedPublic|^|2003|^|1511224917598|^|4295858941|^|30|^|31|^|2|^|O|!|
       SelfSourcedPublic|^|2003|^|1511224917599|^|4295858941|^|30|^|32|^|1|^|O|!|
       SelfSourcedPublic|^|2003|^|1511224917600|^|4295858941|^|30|^|32|^|1|^|O|!|
       SelfSourcedPublic|^|2002|^|1511224917601|^|4295858941|^|24|^|33|^|3|^|O|!|
       SelfSourcedPublic|^|2002|^|1511224917602|^|4295858941|^|24|^|33|^|3|^|O|!|
       SelfSourcedPublic|^|2002|^|1511224917603|^|4295858941|^|24|^|34|^|2|^|O|!|
       SelfSourcedPublic|^|2002|^|1511224917604|^|4295858941|^|24|^|34|^|2|^|O|!|
       SelfSourcedPublic|^|2002|^|1511224917605|^|4295858941|^|1|^|2|^|4|^|O|!|
       SelfSourcedPublic|^|2002|^|1511224917606|^|4295858941|^|1|^|3|^|4|^|O|!|
       SelfSourcedPublic|^|2001|^|1511224917607|^|4295858941|^|5|^|6|^|4|^|O|!|
       SelfSourcedPublic|^|2001|^|1511224917608|^|4295858941|^|5|^|7|^|4|^|O|!|
       SelfSourcedPublic|^|2003|^|1511224917609|^|4295858941|^|12|^|10|^|2|^|O|!|
       SelfSourcedPublic|^|2003|^|1511224917610|^|4295858941|^|12|^|11|^|2|^|O|!|
       SelfSourcedPublic|^|2002|^|1511224917611|^|4295858941|^|1|^|13|^|1|^|O|!|
       SelfSourcedPublic|^|2003|^|1511224917612|^|4295858941|^|12|^|14|^|1|^|O|!|
       SelfSourcedPublic|^|2001|^|1511224917613|^|4295858941|^|5|^|15|^|3|^|O|!|
       SelfSourcedPublic|^|2001|^|1511224917614|^|4295858941|^|5|^|16|^|3|^|O|!|
       SelfSourcedPublic|^|2002|^|1511224917615|^|4295858941|^|1|^|17|^|3|^|O|!|
       SelfSourcedPublic|^|2002|^|1511224917616|^|4295858941|^|1|^|18|^|3|^|O|!|
       SelfSourcedPublic|^|2001|^|1511224917617|^|4295858941|^|5|^|19|^|1|^|O|!|
       SelfSourcedPublic|^|2001|^|1511224917618|^|4295858941|^|5|^|20|^|2|^|O|!|
       SelfSourcedPublic|^|2001|^|1511224917619|^|4295858941|^|5|^|21|^|2|^|O|!|
       SelfSourcedPublic|^|2002|^|1511224917620|^|4295858941|^|1|^|22|^|2|^|O|!|
       SelfSourcedPublic|^|2002|^|1511224917621|^|4295858941|^|1|^|23|^|2|^|O|!|
       SelfSourcedPublic|^|2016|^|1511224917622|^|4295858941|^|35|^|36|^|1|^|I|!|
       SelfSourcedPublic|^|2016|^|1511224917642|^|4295858941|^|null|^|35|^|null|^|D|!|
       SelfSourcedPublic|^|2016|^|1511224917643|^|4295858941|^|null|^|36|^|null|^|D|!|
       SelfSourcedPublic|^|2016|^|1511224917644|^|4295858941|^|null|^|37|^|null|^|D|!|*/


  }
}
