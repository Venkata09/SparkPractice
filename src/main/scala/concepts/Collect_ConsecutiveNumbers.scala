package concepts

/**
  * Created by vdokku on 1/2/2018.
  */
object Collect_ConsecutiveNumbers {


  def main(args: Array[String]): Unit = {


    /**
      * *
      * Whatever the Consecutive numbers will be collected in the lists.
      * *
      *
      * A WHole list of numbers are made into list of lists.
      * List(List(1, 2, 3, 4, 5), List(9), List(11), List(20, 21, 22))
      *
      * @param inputList
      * @return
      */

    def combineConsecutiveNumsToList(inputList: List[Int]): List[List[Int]] = {


      def combine(inputList: List[Int], accumateList: List[Int]): List[List[Int]] = inputList match {
        case Nil => accumateList.reverse :: Nil
        case x :: xs =>
          accumateList match {
            case Nil => combine(xs, x :: accumateList)

            case y :: ys => (x - y) match {
              case 1 => combine(xs, x :: accumateList)
              case _ => accumateList.reverse :: combine(xs, x :: List())
            }
          }
      }

      combine(inputList, List())
    }

    println(combineConsecutiveNumsToList(List(1, 2, 3, 4, 5, 9, 11, 20, 21, 22)))


  }
}
