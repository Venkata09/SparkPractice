import org.apache.spark.sql.SparkSession

/**
  * Created by vdokku on 10/7/2017.
  */
object ListOfLists {


  def main(args: Array[String]): Unit = {

    def f(a: Array[String]): Array[(String, Int)] = {
      scala.util.Sorting.quickSort(a)

      def g(b: List[String]): List[List[String]] = {
        b match {
          case Nil => Nil
          case x :: Nil => List(List(x))
          case h :: tail => {
            val (f, l) = b.span(r => r == h)
            f :: g(l)
          }
        }
      }

      val c = g(a.toList)
      c.map(r => (r.head, r.length)).toArray
    }


    def distinct_Example_1(a: Array[Int]): Array[Int] = {

      a match {
        case Array() => a
        case Array(head, tail@_*) => head +: distinct_Example_1(tail.toArray).filter(_ != head)
      }
    }

    def distinct_Example_2(sampleArray: Array[Int]): Array[Int] = {

      sampleArray match {

        case Array() => sampleArray
        case htail => htail.head +: distinct_Example_2(htail.tail).filter(_ != htail.head)
      }
    }


    /*

    the :: method is used to construct and deconstruct a list. a::b means the head of the list is a (a single element) and the tail is b (a list).

    p::Nil means a case where there is some element p and the tail is the empty list (Nil).

    This case basically finds the last actual element in the list.

    The second case is similar: h::tail means an element h and a list tail.


    So we reverse the tail and then add the list of h to the end (l1 ::: l2 prepends the list l1 to the list l).

     */

    var definingAList = List(1, 2, 3, 4, 5, 6, 76, 43)

    def lastRecursive[A](ls: List[A]): A = ls match {

      case p :: Nil => p
      case _ :: tail => lastRecursive(tail)
      case _ => throw new NoSuchElementException
    }


    def reverseRecursive[A](emptyList: List[A]): List[A] = emptyList match {
      case Nil => Nil
      case h :: tail => reverseRecursive(tail) ::: List(h)
    }


    System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master\\")
    val sparkSession = SparkSession.builder().master("local[4]")
      .appName("my-spark-app")
      .getOrCreate()


    val s = List("hello world scala with spark hello spark scala world", "wow this is wow coz this is wow")
    sparkSession.sparkContext.parallelize(s).map(r => r.split(" ")).map(r => f(r)).map(tupleEntry => tupleEntry.iterator.mkString("\t")).collect().foreach(println)


    // Some more examples:


    val fruits_Ex = List("Banana", "Apple", "Peaches", "Mangoes")
    val numbers = List(1, 2, 3, 4, 5)

    val sample3 = List(List(1, 2, 3, 4), List(2, 4, 5), List(32, 54, 64))

    val emptyList = List()


    // Constructing Lists

    val fruits = "Apples" :: ("Oranges" :: ("Pears" :: Nil))
    println(fruits)

    //List(Apples, Oranges, Pears)

    /*Nil.head
    java.util.NoSuchElementException: head of empty list*/


    val List(a, b, c) = fruits

    /*
      a: String = apples
      b: String = oranges
      c: String = pears
     */

    val d :: e :: rest = fruits

    /*
    d => Apple
    e => Oranges
    rest List[String] => List(Pears)

     */








  }

}



