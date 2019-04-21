package data_analysis_on_sample_data.json

import java.io.FileReader

import org.apache.spark.sql.SparkSession


object Compare {

  def main(args: Array[String]): Unit = {
    val file1 = new FileReader("src/main/resources/sample_data/input1.json")
    val file2 = new FileReader("src/main/resources/sample_data/input2.json")

    val spark = SparkSession.builder().master("local")
      .appName("test").getOrCreate()

    import spark.implicits._


    println(" This is count from the INPUT1 >>>> ", spark.read.json("src/main/resources/sample_data/input1.json")
      .count())

    println("This is count from the INPUT2 >>>> " + spark.read.json("src/main/resources/sample_data/input2.json")
      .count())
  }


}
