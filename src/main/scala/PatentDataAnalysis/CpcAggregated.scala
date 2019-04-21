package PatentDataAnalysis

/**
 * Created by callport on 9/25/15.
 */
case class CpcAggregated(family_identifier_cur:String,
                         id: String,
                         country: String,
                         delta_date: String,
                         application_kind_code: String,
                         cpc_combination_tally_cur: List[_],
                         cpc_combination_classification_cur: List[String],
                         cpc_combination_sets_cur: List[String],
                         cpc_inventive: List[String],
                         cpc_cur_inventive_class: List[String],
                         cpc_cur_additional_class: List[String],
                         cpc_additional: List[String],
                         cpc_cur_classification_group: List[String],
                         cpc_current_inventive_display: List[String],
                         cpc_current_additional_display: List[String]){
  def cpc_additional_flattened(): String = {
    cpc_additional.mkString(";")
  }
  def cpc_inventive_flattened(): String = {
    cpc_inventive.mkString(";")
  }

  def toMap(): Map[String, Any] ={
    Map("cpc_combination_tally_cur" -> cpc_combination_tally_cur.distinct, "cpc_combination_classification_cur" -> cpc_combination_classification_cur.distinct,
            "cpc_combination_sets_cur" -> cpc_combination_sets_cur.distinct, "cpc_cur_classification_group" -> cpc_cur_classification_group.distinct, "cpc_inventive" -> cpc_inventive.distinct,
            "cpc_inventive_flattened" -> cpc_inventive_flattened(),
            "cpc_cur_inventive_class" -> cpc_cur_inventive_class.distinct, "cpc_cur_additional_class" -> cpc_cur_additional_class.distinct,
            "cpc_additional" -> cpc_additional.distinct, "cpc_additional_flattened" -> cpc_additional_flattened() ,"family_identifier_cur" -> family_identifier_cur,
            "cpc_current_inventive_display" -> cpc_current_inventive_display,"cpc_current_additional_display" -> cpc_current_additional_display.distinct)
  }
}

