package DroppableNestedColumns

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.types.{StructType, StructField}
import org.apache.spark.sql.{functions => f}
import scala.util.Try

/**
  * @author vdokku
  */


case class DataFrameWithDroppableForm(dataFrame: DataFrame) {

  def getSourceField(source: String): Try[StructField] = {
    Try(dataFrame.schema.fields.filter(_.name == source).head)
  }

  def getType(sourceField: StructField): Try[StructType] = {
    Try(sourceField.dataType.asInstanceOf[StructType])
  }

  def getOutputColumn(name: Array[String], source: String): Column = {
    f.struct(name.map(x => f.col(source).getItem(x).alias(x)): _*)
  }

  def dropDataFrom(source: String, toDrop: Array[String]): DataFrame = {
    getSourceField(source)
      .flatMap(getType)
      .map(_.fieldNames.diff(toDrop))
      .map(getOutputColumn(_, source))
      .map(dataFrame.withColumn(source, _))
      .getOrElse(dataFrame)
  }

}


case class features(feat1: String, feat2: String, feat3: String)

case class record(str: String, features: features)

object DroppingNestedColumns {
  def main(args: Array[String]): Unit = {


    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")

    val spark: SparkSession = SparkSession.builder().master("local[4]").appName("<<<<<<<<< FL_insurance_sample>>>>>>>>>> ").getOrCreate()
    val sc: SparkContext = spark.sparkContext

    import spark.implicits._

    val df = sc.parallelize(Seq(record("a_label", features("f1", "f2", "f3")))).toDF


    df.show(false)


    DataFrameWithDroppableForm(df).dropDataFrom("features", Array("feat1")).show(false)

    DataFrameWithDroppableForm(df).dropDataFrom("foobar", Array("feat1")).show // Here it did not delete the data.



  }

}
