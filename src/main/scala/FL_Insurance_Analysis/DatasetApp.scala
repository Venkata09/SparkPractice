package FL_Insurance_Analysis

import org.apache.spark.sql.SparkSession

case class Person(name: String, age: Long)

object DatasetApp {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().getOrCreate()

    import spark.implicits._
    // Encoders are created for case classes
    val caseClassDS = Seq(
      Person("Andy", 32),
      Person("Mary", 42),
      Person("Betty", 16)
    ).toDS()
    caseClassDS.show()

    val ds = caseClassDS.map(p => (p.age * 2, p.name)) // woot it infers ds as Dataset[(Long, String)] !
    ds.select("_1").show()
  }

}