package PatentDataAnalysis

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.expressions.Aggregator

/**
  * Created by vdokku on 2/12/2018.
  */
object AggregateCpcData extends Aggregator[Cpc, CpcAggregated, CpcAggregated] with Serializable {


  def checkIfCombinationExists(first: CpcAggregated, second: Cpc): Boolean = {
    var isCombiSetsExists: Boolean = false
    if (second.groupNumber != 0L || second.rankNumber != 0L) {
      isCombiSetsExists = true
    }

    return isCombiSetsExists
  }


  def checkCPCFirstRank(first: CpcAggregated, second: Cpc): Boolean = {
    var isCombiSetsExistsForRank: Boolean = false
    if (second.rankNumber == 1L || first.cpc_combination_sets_cur.size == 0) {
      isCombiSetsExistsForRank = true
    }

    return isCombiSetsExistsForRank
  }


  override def zero: CpcAggregated = new CpcAggregated("", "", "", "", "", List[String](), List[String](), List[String](), List[String](),
    List[String](), List[String](), List[String](), List[String](), List[String](), List[String]())

  override def reduce(first: CpcAggregated, second: Cpc): CpcAggregated = {
    if ("I".equals(second.classificationValueCode)) {
      new CpcAggregated(second.familyId, second.id, second.country, second.deltaDate, second.applicationKindCode,
        if (checkIfCombinationExists(first, second)) {
          first.cpc_combination_tally_cur ::: List(second.getCombinationTally())
        } else first.cpc_combination_tally_cur,
        if (checkIfCombinationExists(first, second)) {
          first.cpc_combination_classification_cur ::: List(second.getCombinationClassificationCur())
        } else first.cpc_combination_classification_cur,
        if (checkIfCombinationExists(first, second)) {
          if (checkCPCFirstRank(first, second)) {
            first.cpc_combination_sets_cur ::: List(second.getCombinationSetsCur())
          } else {

            first.cpc_combination_sets_cur.take(first.cpc_combination_sets_cur.size - 1) :::
              List(first.cpc_combination_sets_cur.last +
                (
                  if (second.getCombinationSetsCur().size > 0)
                    (", " + second.getCombinationSetsCur())
                  else "")
              )
          }
        } else first.cpc_combination_sets_cur,
        first.cpc_inventive ::: List(second.getCpcInventive()),
        first.cpc_cur_inventive_class ::: List(second.getCpcCurInventive()), first.cpc_cur_additional_class, first.cpc_additional,
        first.cpc_cur_classification_group ::: List(second.getCpcCurClassificationGroup()),
        first.cpc_current_inventive_display ::: List(second.getCpcInventiveDisplay()),
        first.cpc_current_additional_display)
    } // A indicates additional, so we combine in the additional fields to the aggregated object
    else if ("A".equals(second.classificationValueCode)) {
      new CpcAggregated(second.familyId, second.id, second.country, second.deltaDate, second.applicationKindCode,
        if (checkIfCombinationExists(first, second)) {
          first.cpc_combination_tally_cur ::: List(second.getCombinationTally())
        } else first.cpc_combination_tally_cur,
        if (checkIfCombinationExists(first, second)) {
          first.cpc_combination_classification_cur ::: List(second.getCombinationClassificationCur())
        } else first.cpc_combination_classification_cur,
        if (checkIfCombinationExists(first, second)) {
          if (checkCPCFirstRank(first, second)) {
            first.cpc_combination_sets_cur ::: List(second.getCombinationSetsCur())
          } else {

            first.cpc_combination_sets_cur.take(first.cpc_combination_sets_cur.size - 1) :::
              List(first.cpc_combination_sets_cur.last +
                (
                  if (second.getCombinationSetsCur().size > 0)
                    (", " + second.getCombinationSetsCur())
                  else "")
              )
          }
        } else first.cpc_combination_sets_cur,
        first.cpc_inventive,
        first.cpc_cur_inventive_class,
        first.cpc_cur_additional_class ::: List(second.getCpcCurAdditional()),
        first.cpc_additional ::: List(second.getCpcAdditional()),
        first.cpc_cur_classification_group ::: List(second.getCpcCurClassificationGroup()),
        first.cpc_current_inventive_display,
        first.cpc_current_additional_display ::: List(second.getCpcAdditionalDisplay()))
    } else {
      //if not additional or inventive, we just need to append the cur_classification_group
      new CpcAggregated(second.familyId, second.id, second.country, second.deltaDate, second.applicationKindCode, first.cpc_combination_tally_cur,
        first.cpc_combination_classification_cur, first.cpc_combination_sets_cur, first.cpc_inventive,
        first.cpc_cur_inventive_class, first.cpc_cur_additional_class,
        first.cpc_additional,
        first.cpc_cur_classification_group ::: List(second.getCpcCurClassificationGroup()),
        first.cpc_current_inventive_display,
        first.cpc_current_additional_display)
    }
  }

  override def merge(first: CpcAggregated, second: CpcAggregated): CpcAggregated = {
    new CpcAggregated(first.family_identifier_cur, first.id, first.country, second.delta_date, first.application_kind_code, first.cpc_combination_tally_cur ::: second.cpc_combination_tally_cur,
      first.cpc_combination_classification_cur ::: second.cpc_combination_classification_cur, first.cpc_combination_sets_cur ::: second.cpc_combination_sets_cur,
      first.cpc_inventive ::: second.cpc_inventive,
      first.cpc_cur_inventive_class ::: second.cpc_cur_inventive_class, first.cpc_cur_additional_class ::: second.cpc_cur_additional_class,
      first.cpc_additional ::: second.cpc_additional,
      first.cpc_cur_classification_group ::: second.cpc_cur_classification_group,
      first.cpc_current_inventive_display ::: second.cpc_current_inventive_display,
      first.cpc_current_additional_display ::: second.cpc_current_additional_display)
  }

  override def finish(reduction: CpcAggregated): CpcAggregated = reduction

  override def bufferEncoder: Encoder[CpcAggregated] = org.apache.spark.sql.Encoders.product

  override def outputEncoder: Encoder[CpcAggregated] = org.apache.spark.sql.Encoders.product
}
