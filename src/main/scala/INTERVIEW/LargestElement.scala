package Interview

/**
  * @author vdokku
  */
object LargestElement {

  def main(args: Array[String]): Unit = {
    var max_diff = 0
    var myList = Array(2, 3, 10, 6, 4, 8, 1)
    for (i <- 0 to (myList.length - 2)) {
      if (i != 0 && (myList(i + 1) - myList(i)) > max_diff) {
        max_diff = myList(i + 1) - myList(i)
      } else if (i == 0) {
        max_diff = myList(i + 1) - myList(i)
      }
    }

    println(max_diff)

    //Find 3rd Largest element in array
    var nrd = 3
    var OnestElem = myList(0)
    var secElem = Int.MinValue
    var thrElem = Int.MinValue

    for (j <- 1 until myList.length) {
      if (OnestElem < myList(j)) {
        /* Since the FIRST position got Changed .... Follow up positions for the second & Third one also changes. */
        thrElem = secElem
        secElem = OnestElem
        OnestElem = myList(j)
      } else if (secElem < myList(j)) {
        /* Since the SECOND element got changed. Follow up postion of Third Element will be changed.*/
        thrElem = secElem
        secElem = myList(j)
      } else if (thrElem < myList(j)) {
        thrElem = myList(j)
      }
    }

    println("3rd largest Element = " + thrElem + " - " + secElem + " - " + OnestElem)

  }

}
