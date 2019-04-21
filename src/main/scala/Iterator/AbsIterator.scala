package Iterator

/**
  * Created by vdokku on 1/3/2018.
  */
abstract class AbsIterator {
  type T

  def hasNext: Boolean

  def next: T
}
