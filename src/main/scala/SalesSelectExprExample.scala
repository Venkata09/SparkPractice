import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Literal, Multiply}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by vdokku on 1/3/2018.
  */
object SalesSelectExprExample {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")


    val sparkSession = SparkSession.builder().master("local[4]")
      .appName("<<<< Slaes Example - Select EXPR >>>>>")
      .getOrCreate()

    val sc = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext

    val schema = StructType(
      Array(StructField("transactionId", StringType),
        StructField("customerId", StringType),
        StructField("itemId", StringType),
        StructField("amountPaid", StringType)))

    val salesData = sparkSession.read.option("header", "true").
      csv("src/main/resources/datamantra/sales.csv")

    salesData.show()

    salesData.selectExpr(
      "transactionId", "customerId", "itemId", "amountPaid", "'Venkata' as saludo", "'HELLO123' as saludo1").show()


    val multipliedSalesDataDF = salesData.selectExpr("amountPaid * 1")


    println(multipliedSalesDataDF.queryExecution.optimizedPlan.numberedTreeString)


    // Now add out own custom optimizations
    sparkSession.experimental.extraOptimizations = Seq(MultiplyOptimizationRule)
    val multipliedDFWithCustomOptimization = salesData.selectExpr("amountPaid * 1")
    println("after optimization")

    println(multipliedDFWithCustomOptimization.queryExecution.optimizedPlan.numberedTreeString)


  }

  object MultiplyOptimizationRule extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      case Multiply(left, right) if right.isInstanceOf[Literal] &&
        right.asInstanceOf[Literal].value.asInstanceOf[Double] == 1.0 =>
        println("optimization of one applied")
        left
    }
  }

}
