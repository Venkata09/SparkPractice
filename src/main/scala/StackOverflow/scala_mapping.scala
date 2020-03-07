package StackOverflow

import org.apache.commons.lang3.StringUtils

object scala_mapping {

  case class DDD(abc: String, v1: String, v2: String)

  def isNotEmpty(x: Any) = x != null || !x.toString.isEmpty || StringUtils.isNotBlank(x.toString)


  def isEmpty(x: Any) = x == null || x.toString.isEmpty || StringUtils.isBlank(x.toString)


  def getCCParams1(cc: AnyRef) =
    (Map[String, Any]() /: cc.getClass.getDeclaredFields) { (a, f) =>
      f.setAccessible(true)
      a + (f.getName -> f.get(cc))
    }

  def getCCParams(cc: DDD): Map[String, Any] = {
    val values = cc.productIterator
    cc.getClass.getDeclaredFields.map {
      _.getName -> (values.next() match {
        case x if isNotEmpty(x) => x
        case x if isEmpty(x) => None
      })
    }.toMap
  }

  def main(args: Array[String]): Unit = {

    val derivedOutput = getCCParams(DDD("test", "", ""))


    println(derivedOutput)

    /*
        case class Person(name: String, surname: String)
        val person = new Person("Daniele", "DalleMule")


        val personAsMap = person.getClass.getDeclaredFields.foldLeft(Map[String, Any]())((map, field) => {
          field.setAccessible(true) map +(field.getName -> field.get(person))
        })



        val sample1 = DDD("a1", "v0001", "v0002")


        val outputMap = getCCParams(Product(""))

        val tMap = Map("col_name" -> sample1.abc, "col_old" -> sample1.v1, "col_new" -> sample1.v2)
    */


  }
}
