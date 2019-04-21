package UDFS_Example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object UDFTest {


  def main(args: Array[String]): Unit = {

    /*
{"name":"Michael","age":15,"sal":1.00}
{"name":"Andy", "age":30,"sal":1.00}
{"name":"Justin", "age":19,"sal":1.00}
     */

    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")
    val sparkSession = SparkSession.builder().master("local[4]")
      .appName("UDFTest ---------------> ")
      .getOrCreate()

    import sparkSession.implicits._
    val peoplesDataFrame = sparkSession.read.json("src/main/resources/data/people.json")


    val incrementByOne = (i: Int) => i + 1

    val addByOneFunction = udf(incrementByOne)

    peoplesDataFrame.select(addByOneFunction($"age") as "test1D").show()

    val addOneByRegister = sparkSession.udf.register("addOneByRegister", addByOneFunction)

    peoplesDataFrame.selectExpr(" addOneByRegister(age) as test2S ").show()


    peoplesDataFrame.select(callUDF("addOneByRegister", $"age") as "test2CallUDF").show()


    // How to pass literal values to UDF ?

    val addByLit = udf((i: Int, x: Int) => x + i)


    //lit is like passing the LITERAL value to the DATAFRAME.
    peoplesDataFrame.select(addByLit(lit(3), $"age") as "testLitF").show()

  }

}
