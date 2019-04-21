package SortING


import scala.collection.mutable.ArrayBuffer


object HeapSort {

  def left(i: Int) = 2 * i

  def right(i: Int) = 2 * i + 1

  def parent(i: Int) = i / 2


  def swap(xs: ArrayBuffer[Int], m: Int, n: Int) = {
    val t = xs(m)
    xs.update(m, xs(n))
    xs.update(n, t)
  }


  def maxHeapfy(xs: ArrayBuffer[Int], i: Int, heapSize: Int): Unit = {
    val leftIdx = left(i)
    val rightIdx = right(i)


    var largestIdx: Int = i

    if (leftIdx < heapSize && xs(leftIdx) > xs(largestIdx)) {
      largestIdx = leftIdx
    }
    if (rightIdx < heapSize && xs(rightIdx) > xs(largestIdx)) {
      largestIdx = rightIdx
    }
    if (largestIdx != i) {
      swap(xs, largestIdx, i)
      maxHeapfy(xs, largestIdx, heapSize)
    }
  }

  def buildMaxHeap(xs: ArrayBuffer[Int], heapSize: Int): Unit = {
    for (i <- parent(xs.length) to 0 by -1) {
      maxHeapfy(xs, i, heapSize)
    }
  }

  def heapSort(xs: ArrayBuffer[Int]): Unit = {
    buildMaxHeap(xs, heapSize = xs.length)
    var heapSize = xs.length
    for (i <- xs.length - 1 to 1 by -1) {
      swap(xs, 0, i)
      heapSize = heapSize - 1
      maxHeapfy(xs, 0, heapSize)
    }
  }

  def main(args: Array[String]): Unit = {
    // when list is small
    val buffer = new ArrayBuffer[Int]()
    (1 to 10000).map(x => (Math.random() * 1000 + 1).toInt).foreach(x => buffer.append(x))
    //    List(248, 959, 545, 204, 760, 403, 600, 504, 482, 91).foreach(x => buffer.append(x))
    println(buffer.toList)
    val time = System.currentTimeMillis()
    heapSort(buffer)
    println(buffer.toList)
    println(s"duration: ${System.currentTimeMillis() - time}ms") // 10000element in 75ms
  }

}
