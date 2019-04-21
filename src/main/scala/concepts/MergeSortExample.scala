package concepts

/**
  * Created by vdokku on 2/5/2018.
  */
object MergeSortExample {


  def merge(leftList: List[Int], rightList: List[Int]): List[Int] =
    (leftList, rightList) match {
      case (left, Nil) => left
      case (Nil, right) => right
      case (leftHead :: leftTail, rightHead :: rightTail) =>
        if (leftHead < rightHead)
          leftHead :: merge(leftTail, rightList)
        else
          rightHead :: merge(leftList, rightTail)
    }


  def merge_1(left: List[Int], right: List[Int]): List[Int] =
    (left, right) match {
      case (_, Nil) => left
      case (Nil, _) => right
      case (leftHead :: leftTail, rightHead :: rightTail) =>
        if (leftHead < rightHead) leftHead :: merge(leftTail, right)
        else rightHead :: merge(left, rightTail)
    }

  def mergeSort(inputList: List[Int]): List[Int] = {
    val n = inputList.length / 2
    if (n == 0) inputList
    else {
      val (left, right) = inputList.splitAt(n) // override def splitAt(n: Int): (List[A], List[A]) = {
      merge(mergeSort(left), mergeSort(right))
    }
  }


  def main(args: Array[String]): Unit = {

    val sortedList = mergeSort(List(12, 5, 23, 6432, 75, 23))

    sortedList.foreach(entry => print(entry + ",  "))


  }


}
