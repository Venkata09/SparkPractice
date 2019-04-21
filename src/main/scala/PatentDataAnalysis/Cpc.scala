package PatentDataAnalysis

import org.apache.commons.lang.StringUtils

/**
 * Created by vdokku on 9/25/15.
 */
case class Cpc(familyId: String, id: String, country: String,
               deltaDate: String,
               applicationKindCode: String,
               rankNumber: Long,
               groupNumber: Long,
               classificationValueCode: String,
               positionNumber: String,
               sectionCode: String,
               classCode: String,
               subClassCode: String,
               mainGroupCode: String,
               classificationMethodCode: String,
               publicationDate: String,
               subGroupCode: String,
               schemeVersionDate: String,
               classificationStatusCode: String,
               cpcMemberCountryCode: String
                ) {

  def getCombinationTally(): String = {
    if (!(groupNumber == 0L) || !(rankNumber == 0L)) {
      StringUtils.trimToEmpty(sectionCode) + StringUtils.trimToEmpty(classCode) +
        StringUtils.trimToEmpty(subClassCode) + StringUtils.trimToEmpty(mainGroupCode) + "/" + StringUtils.trimToEmpty(subGroupCode)
    }
    else {
      ""
    }
  }

  def getCombinationClassificationCur(): String = {
    if (!(groupNumber == 0L) || !(rankNumber == 0L)) {
      groupNumber.toString + " " + rankNumber.toString + " " +
        StringUtils.trimToEmpty(sectionCode) + " " + StringUtils.trimToEmpty(sectionCode) +
        StringUtils.trimToEmpty(classCode) + StringUtils.trimToEmpty(subClassCode) +
        " " + StringUtils.trimToEmpty(sectionCode) + StringUtils.trimToEmpty(classCode) +
        StringUtils.trimToEmpty(subClassCode) + StringUtils.trimToEmpty(mainGroupCode) +
        "/" + StringUtils.trimToEmpty(subGroupCode) +
        " " + StringUtils.trimToEmpty(schemeVersionDate) + " " + convertPosition() + " " +
        StringUtils.trimToEmpty(classificationValueCode) + " " + StringUtils.trimToEmpty(classificationStatusCode) +
        " " + StringUtils.trimToEmpty(classificationMethodCode) + " " + StringUtils.trimToEmpty(deltaDate) +
        " " + StringUtils.trimToEmpty(cpcMemberCountryCode)
    }
    else {
      ""
    }
  }

  def convertPosition(): String = {
    if (positionNumber.toInt > 1) {
      "L"
    }
    else {
      "F"
    }
  }

  //NOTE: may need to change this.  The incremental version puts the same data in twice w/ a comma and a space separating them
  def getCombinationSetsCur(): String = {
    if (!(groupNumber == 0L) || !(rankNumber == 0L)) {
      rankNumber.toString + " " + StringUtils.trimToEmpty(sectionCode) + StringUtils.trimToEmpty(classCode) + StringUtils.trimToEmpty(subClassCode) + StringUtils.trimToEmpty(mainGroupCode) + "/" + StringUtils.trimToEmpty(subGroupCode)
    }
    else {
      ""
    }
  }

  def getCpcInventive(): String = {
    if ("I".equals(classificationValueCode)) {
      StringUtils.trimToEmpty(sectionCode) + StringUtils.trimToEmpty(classCode) + StringUtils.trimToEmpty(subClassCode) +
        StringUtils.trimToEmpty(mainGroupCode) + "/" + StringUtils.trimToEmpty(subGroupCode) + " " + StringUtils.trimToEmpty(schemeVersionDate)
    }
    else {
      ""
    }
  }

  def getCpcInventiveDisplay(): String = {
    if ("I".equals(classificationValueCode)) {
      StringUtils.trimToEmpty(sectionCode) + StringUtils.trimToEmpty(classCode) + StringUtils.trimToEmpty(subClassCode) +
        StringUtils.trimToEmpty(mainGroupCode) + "/" + StringUtils.trimToEmpty(subGroupCode)
    }
    else {
      ""
    }
  }

  def getCpcCurInventive(): String = {
    if ( "I".equals(classificationValueCode)) {
      StringUtils.trimToEmpty(sectionCode) + StringUtils.trimToEmpty(classCode) + StringUtils.trimToEmpty(subClassCode)
    }
    else {
      ""
    }
  }

  def getCpcAdditional(): String = {
    if ("A".equals(classificationValueCode)) {
      StringUtils.trimToEmpty(sectionCode) + StringUtils.trimToEmpty(classCode) + StringUtils.trimToEmpty(subClassCode) +
        StringUtils.trimToEmpty(mainGroupCode) + "/" + StringUtils.trimToEmpty(subGroupCode) + " " + StringUtils.trimToEmpty(schemeVersionDate)
    }
    else {
      ""
    }
  }

  def getCpcAdditionalDisplay(): String = {
    if ( "A".equals(classificationValueCode)) {
      StringUtils.trimToEmpty(sectionCode) + StringUtils.trimToEmpty(classCode) + StringUtils.trimToEmpty(subClassCode) +
        StringUtils.trimToEmpty(mainGroupCode) + "/" + StringUtils.trimToEmpty(subGroupCode)
    }
    else {
      ""
    }
  }

  def getCpcCurAdditional(): String = {
    if ( "A".equals(classificationValueCode)) {
      StringUtils.trimToEmpty(sectionCode) + StringUtils.trimToEmpty(classCode) + StringUtils.trimToEmpty(subClassCode)
    }
    else {
      ""
    }
  }

  def getCpcCurClassificationGroup(): String = {
      StringUtils.trimToEmpty(sectionCode) + " " + StringUtils.trimToEmpty(sectionCode) + StringUtils.trimToEmpty(classCode) + StringUtils.trimToEmpty(subClassCode) +
        " " + StringUtils.trimToEmpty(sectionCode) + StringUtils.trimToEmpty(classCode) +
        StringUtils.trimToEmpty(subClassCode) + StringUtils.trimToEmpty(mainGroupCode) + "/" + StringUtils.trimToEmpty(subGroupCode) + " " +
        StringUtils.trimToEmpty(schemeVersionDate) + " " + convertPosition() + " " + StringUtils.trimToEmpty(classificationValueCode) +
        " " + StringUtils.trimToEmpty(classificationStatusCode) + " " + StringUtils.trimToEmpty(classificationMethodCode) + " " +
        StringUtils.trimToEmpty(deltaDate) + " " + StringUtils.trimToEmpty(cpcMemberCountryCode)
  }

}


