package StackOverflow

import org.apache.spark.sql.SparkSession


case class ProductFeatures(featureList: String, value: String)

case class record(visitorId: String, products: List[ProductFeatures])

object DF_Joins {


  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession.builder().master("local").appName("test").getOrCreate()

    import spark.implicits._


    val prodFeatures1 = ProductFeatures("i1", "0.68")
    val prodFeatures2 = ProductFeatures("i2", "0.42")
    val prodFeatures3 = ProductFeatures("i1", "0.78")
    val prodFeatures4 = ProductFeatures("i3", "0.11")


    val jsonDataWithArray = spark.read.json(spark.sparkContext.parallelize(Seq("""{
                                                                                 |    "visitorId": "v1",
                                                                                 |    "products": [{
                                                                                 |         "id": "i1",
                                                                                 |         "name": "Nike Shoes",
                                                                                 |         "interest": 0.68
                                                                                 |    }, {
                                                                                 |         "id": "i2",
                                                                                 |         "name": "Umbrella",
                                                                                 |         "interest": 0.42
                                                                                 |    }]
                                                                                 |},
                                                                                 |{
                                                                                 |    "visitorId": "v2",
                                                                                 |    "products": [{
                                                                                 |         "id": "i1",
                                                                                 |         "name": "Nike Shoes",
                                                                                 |         "interest": 0.78
                                                                                 |    }, {
                                                                                 |         "id": "i3",
                                                                                 |         "name": "Jeans",
                                                                                 |         "interest": 0.11
                                                                                 |    }]
                                                                                 |}""")))


    jsonDataWithArray.show()




    println("------------------------------------------------------------------")


    /*
    +----------------------+---------+
    |products              |visitorId|
    +----------------------+---------+
    |[[i1,0.68], [i2,0.42]]|v1       |
    |[[i1,0.78], [i3,0.11]]|v2       |
    +----------------------+---------+


+---+----------+
| id|  products|
+---+----------+
| i1|Nike Shoes|
| i2|  Umbrella|
| i3|     Jeans|
+---+----------+

+------------------------------------------+---------+
|products                                  |visitorId|
+------------------------------------------+---------+
|[[i1,0.68,Nike Shoes], [i2,0.42,Umbrella]]|v1       |
|[[i1,0.78,Nike Shoes], [i3,0.11,Jeans]]   |v2       |
+------------------------------------------+---------+




     */


    println(" <<<<<<<<<<<<<<<<< AFTER FLATTENING >>>>>>>>>>>>>>>>>>>>>>>>>>> ")


  }
}
