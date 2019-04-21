package Iterator

/**
  * Created by vdokku on 1/3/2018.
  */
class StringIterator(s: String) extends AbsIterator {
  type T = Char
  private var i = 0

  def hasNext: Boolean = i < s.length

  def next = {
    val ch = s charAt i
    i += 1
    ch
  }
}
