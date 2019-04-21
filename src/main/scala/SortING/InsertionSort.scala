package SortING

import org.apache.spark.sql.SparkSession

/**
  * Created by vdokku on 10/8/2017.
  */
object InsertionSort {

  def main(args: Array[String]): Unit = {



    def isort(sampleList: List[Int]): List[Int] = sampleList match {

      case List() => List()
      case x :: xs1 => insert(x, isort(xs1))
    }


    def insert(i: Int, ints: List[Int]): List[Int] = ints match {
      case List() => List(i)
      case y :: ys => if (i < y) i :: ints else y :: insert(i, ys)
    }


//    ::: -> For adding the two lists.

    println("isort(List(5, 3, 12)) [" + isort(List(5, 3, 12)) + "]")






  }
}
