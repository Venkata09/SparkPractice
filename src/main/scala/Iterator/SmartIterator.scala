package Iterator


/**
  * Created by vdokku on 1/3/2018.
  */
trait SmartIterator extends AbsIterator {
  def foreach(f: T => Unit) {
    while (hasNext) f(next)
  }
}
