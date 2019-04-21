package PatentDataAnalysis


import org.apache.commons.lang.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession


case class MapCSVToCassandra(DESIGNATED_FAMILY_ID: String, PUB_SEQ_NO: String, PUB_IPO_CD: String, PUB_KIND_CD: String, POSITION_NO: String, RANK_NO: String, SECTION_CD: String, CLASS_CD: String, SUBCLASS_CD: String,
                             MAINGROUP_CD: String, GROUP_NO: String, SUBGROUP_CD: String, SCHEME_VERSION_DT: String, CLASSIFICATION_VALUE_CD: String, CLASSIFICATION_STATUS_CD: String, CLASSIFICATION_METHOD_CD: String,
                             PUB_DT: String, ACTION_DT: String) {

  def getCombinationTally(): String = {
    if (!(GROUP_NO.equalsIgnoreCase("0")) || !(RANK_NO.equalsIgnoreCase("0"))) {
      StringUtils.trimToEmpty(SECTION_CD) + StringUtils.trimToEmpty(CLASS_CD) +
        StringUtils.trimToEmpty(SUBCLASS_CD) + StringUtils.trimToEmpty(MAINGROUP_CD) + "/" + StringUtils.trimToEmpty(SUBGROUP_CD)
    }
    else {
      ""
    }
  }

  def getCpcInventiveDisplay(): String = {
    if ("I".equals(CLASSIFICATION_VALUE_CD)) {
      StringUtils.trimToEmpty(SECTION_CD) + StringUtils.trimToEmpty(CLASS_CD) + StringUtils.trimToEmpty(SUBCLASS_CD) +
        StringUtils.trimToEmpty(MAINGROUP_CD) + "/" + StringUtils.trimToEmpty(SUBGROUP_CD)
    }
    else {
      ""
    }
  }

  def getCpcAdditionalDisplay(): String = {
    if ("A".equals(CLASSIFICATION_VALUE_CD)) {
      StringUtils.trimToEmpty(SECTION_CD) + StringUtils.trimToEmpty(CLASS_CD) + StringUtils.trimToEmpty(SUBCLASS_CD) +
        StringUtils.trimToEmpty(MAINGROUP_CD) + "/" + StringUtils.trimToEmpty(SUBGROUP_CD)
    }
    else {
      ""
    }
  }

  def getCombinationClassificationCur(): String = {
    if (!(GROUP_NO.equalsIgnoreCase("0")) || !(RANK_NO.equalsIgnoreCase("0"))) {
      GROUP_NO.toString + " " + RANK_NO.toString + " " +
        StringUtils.trimToEmpty(SECTION_CD) + " " + StringUtils.trimToEmpty(SECTION_CD) +
        StringUtils.trimToEmpty(CLASS_CD) + StringUtils.trimToEmpty(SUBCLASS_CD) +
        " " + StringUtils.trimToEmpty(SECTION_CD) + StringUtils.trimToEmpty(CLASS_CD) +
        StringUtils.trimToEmpty(SUBCLASS_CD) + StringUtils.trimToEmpty(MAINGROUP_CD) +
        "/" + StringUtils.trimToEmpty(SUBGROUP_CD) +
        " " + StringUtils.trimToEmpty(SCHEME_VERSION_DT) + " " + convertPosition() + " " +
        StringUtils.trimToEmpty(CLASSIFICATION_VALUE_CD) + " " + StringUtils.trimToEmpty(CLASSIFICATION_STATUS_CD) +
        " " + StringUtils.trimToEmpty(CLASSIFICATION_METHOD_CD) + " " + StringUtils.trimToEmpty(ACTION_DT) +
        " " + StringUtils.trimToEmpty(PUB_IPO_CD)
    }
    else {
      ""
    }
  }

  def convertPosition(): String = {
    if (POSITION_NO.toInt > 1) {
      "L"
    }
    else {
      "F"
    }
  }

  //NOTE: may need to change this.  The incremental version puts the same data in twice w/ a comma and a space separating them
  def getCombinationSetsCur(): String = {
    if (!(GROUP_NO.equalsIgnoreCase("0")) || !(RANK_NO.equalsIgnoreCase("0"))) {
      RANK_NO.toString + " " + StringUtils.trimToEmpty(SECTION_CD) + StringUtils.trimToEmpty(CLASS_CD) + StringUtils.trimToEmpty(SUBCLASS_CD) + StringUtils.trimToEmpty(MAINGROUP_CD) + "/" + StringUtils.trimToEmpty(SUBGROUP_CD)
    }
    else {
      ""
    }
  }

  def getCpcInventive(): String = {
    if ("I".equals(CLASSIFICATION_VALUE_CD)) {
      StringUtils.trimToEmpty(SECTION_CD) + StringUtils.trimToEmpty(CLASS_CD) + StringUtils.trimToEmpty(SUBCLASS_CD) +
        StringUtils.trimToEmpty(MAINGROUP_CD) + "/" + StringUtils.trimToEmpty(SUBGROUP_CD) + " " + StringUtils.trimToEmpty(SCHEME_VERSION_DT)
    }
    else {
      ""
    }
  }

  def getCpcCurInventive(): String = {
    if ("I".equals(CLASSIFICATION_VALUE_CD)) {
      StringUtils.trimToEmpty(SECTION_CD) + StringUtils.trimToEmpty(CLASS_CD) + StringUtils.trimToEmpty(SUBCLASS_CD)
    }
    else {
      ""
    }
  }

  def getCpcAdditional(): String = {
    if ("A".equals(CLASSIFICATION_VALUE_CD)) {
      StringUtils.trimToEmpty(SECTION_CD) + StringUtils.trimToEmpty(CLASS_CD) + StringUtils.trimToEmpty(SUBCLASS_CD) +
        StringUtils.trimToEmpty(MAINGROUP_CD) + "/" + StringUtils.trimToEmpty(SUBGROUP_CD) + " " + StringUtils.trimToEmpty(SCHEME_VERSION_DT)
    }
    else {
      ""
    }
  }

  def getCpcCurAdditional(): String = {
    if ("A".equals(CLASSIFICATION_VALUE_CD)) {
      StringUtils.trimToEmpty(SECTION_CD) + StringUtils.trimToEmpty(CLASS_CD) + StringUtils.trimToEmpty(SUBCLASS_CD)
    }
    else {
      ""
    }
  }

  def getCpcCurClassificationGroup(): String = {
    StringUtils.trimToEmpty(SECTION_CD) + " " + StringUtils.trimToEmpty(SECTION_CD) + StringUtils.trimToEmpty(CLASS_CD) + StringUtils.trimToEmpty(SUBCLASS_CD) +
      " " + StringUtils.trimToEmpty(SECTION_CD) + StringUtils.trimToEmpty(CLASS_CD) +
      StringUtils.trimToEmpty(SUBCLASS_CD) + StringUtils.trimToEmpty(MAINGROUP_CD) + "/" + StringUtils.trimToEmpty(SUBGROUP_CD) + " " +
      StringUtils.trimToEmpty(SCHEME_VERSION_DT) + " " + convertPosition() + " " + StringUtils.trimToEmpty(CLASSIFICATION_VALUE_CD) +
      " " + StringUtils.trimToEmpty(CLASSIFICATION_STATUS_CD) + " " + StringUtils.trimToEmpty(CLASSIFICATION_METHOD_CD) + " " +
      StringUtils.trimToEmpty(ACTION_DT) + " " + StringUtils.trimToEmpty(PUB_IPO_CD)
  }

}


/**
  * Created by vdokku on 2/9/2018.
  */
object DataAnalysis {


  def main(args: Array[String]): Unit = {


    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")


    val spark: SparkSession = SparkSession.builder().master("local[4]").appName("<<<<<<<<<<<< ByKeyImplementation>>>>>>>>>>> ").getOrCreate()
    val sc: SparkContext = spark.sparkContext

    import spark.implicits._

    val dataSetEx = spark.read
      .option("header", "false")
      /*.option("inferSchema","false")*/
      .csv("C:\\Phase_5\\DSE_Upgrade_Input_Files\\CPC-export-36-of-40.csv")


    //    |     _c0|            _c1|_c2|_c3|_c4|_c5|_c6|_c7|_c8| _c9|_c10|  _c11|    _c12|_c13|_c14|_c15|_c16|    _c17|
    dataSetEx.show(3)

    val refinedDataSet = dataSetEx.select(
      $"_c0".alias("DESIGNATED_FAMILY_ID"),
      $"_c1".alias("PUB_SEQ_NO"),
      $"_c2".alias("PUB_IPO_CD"),
      $"_c3".alias("PUB_KIND_CD"),
      $"_c4".alias("POSITION_NO"),
      $"_c5".alias("RANK_NO"),
      $"_c6".alias("SECTION_CD"),
      $"_c7".alias("CLASS_CD"),
      $"_c8".alias("SUBCLASS_CD"),
      $"_c9".alias("MAINGROUP_CD"),
      $"_c10".alias("GROUP_NO"),
      $"_c11".alias("SUBGROUP_CD"),
      $"_c12".alias("SCHEME_VERSION_DT"),
      $"_c13".alias("CLASSIFICATION_VALUE_CD"),
      $"_c14".alias("CLASSIFICATION_STATUS_CD"),
      $"_c15".alias("CLASSIFICATION_METHOD_CD"),
      $"_c16".alias("PUB_DT"),
      $"_c17".alias("ACTION_DT"))

    refinedDataSet.select("PUB_SEQ_NO").show()

    refinedDataSet.createOrReplaceTempView("uspat_fields_view")

    val groupByyearDataFrame = refinedDataSet.groupBy($"year").count()


    import spark.implicits._

    val refinedGroupedRDD = refinedDataSet.groupBy("PUB_SEQ_NO", "PUB_IPO_CD", "PUB_KIND_CD", "PUB_DT",
      if ("PUB_DT" != null && "".trim.length == 8) "PUB_DT".substring(0, 4) else "1234")


    refinedGroupedRDD


    println("Dataset Count >>>>> ", refinedDataSet.count())


  }


}
