package concepts

import org.apache.spark.sql.SparkSession

/**
  * Created by vdokku on 2/16/2018.
  */
class Encoders_Spark {

  /*


  Encoder are similar to the SER-DE concept in HIVE.
  Serialization and de-serialization concepts....




   */
  case class Person(id: Long, name: String)


  /*
  countryCode: String , kindCode: String , documentId: String , docCreateTimeStamp: String , docModificationTime: String , designatedFamilyId: String , fullSymbolText: String , sectionCode String , classCode: String , subclassCode: String , maingroupCode: String , subgroupCode: String , schemeVersionDate: String , symbolStatusCoded: String , groupNumber: String , rankNumber: String , cpcMemberCountryCode: String , actionDate: String , classificationMethodCode: String , positionNumber: String , classificationValueCode: String , commonClassificationIndicator: String , classificationStatusCode: String , allocCreateTimestamp: String , allocModificationTimestamp: String , familyIdentifier: String , withdrawnIndicator: String , datePublInt: String
   */

  case class CpcExtractView(countryCode: String, kindCode: String, documentId: String, docCreateTimeStamp: String,
                            docModificationTime: String, designatedFamilyId: String, fullSymbolText: String, sectionCode: String,
                            classCode: String, subclassCode: String, maingroupCode: String, subgroupCode: String, schemeVersionDate: String,
                            symbolStatusCoded: String, groupNumber: String, rankNumber: String, cpcMemberCountryCode: String, actionDate: String,
                            classificationMethodCode: String, positionNumber: String, classificationValueCode: String, commonClassificationIndicator: String,
                            classificationStatusCode: String, allocCreateTimestamp: String, allocModificationTimestamp: String, familyIdentifier: String,
                            withdrawnIndicator: String, datePublInt: String)

  case class CpcFamilyActionView(actualActionDate: String, familyId: String, actionTimeStamp: String)

  def main(args: Array[String]): Unit = {


    val sparkSession = SparkSession.builder().appName("DataFrame VS DataSet").master("local[4]")
      .appName("<<<< GroupByExample  >>>>>")
      .getOrCreate()

    val sc = sparkSession.sparkContext


    val familyActionQuery = "select * from " + "schemaName" + ".FAMILY_ACTION_V"
    val cpcExtractViewQuery = "select * FROM  " + "schemaName" + ".CPC_CSS_EXTRACT_V"
    val familyActionDataset = sparkSession.sql(familyActionQuery)/*.as[CpcExtractView]*/
    val cpcExtractDataset = sparkSession.sql(cpcExtractViewQuery)/*.as[CpcFamilyActionView]*/

    cpcExtractDataset.createOrReplaceTempView("Family_Action_View")

    val exampleQuery = "select distinct(familyId) from Family_Action_View where " +
      "actualActionDate >= TO_DATE('20180101', 'yyyymmdd') and ACTION_DT <= TO_DATE('20180202', 'yyyymmdd') "

    val executingSampleQuery = sparkSession.sql(exampleQuery)

    executingSampleQuery.show()



    cpcExtractDataset.printSchema()


    familyActionDataset.show()

  }

}
