package Iterator

/**
  * Created by vdokku on 1/3/2018.
  */
object StringIteratorTest {
  def main(args: Array[String]) {
    class Iter extends StringIterator(args(0)) with SmartIterator
    val iter = new Iter
    iter foreach println
  }


  def matchTest(x: Int): String = x match {
    case 1 => "one"
    case 2 => "two"
    case _ => "many"
  }

  println(matchTest(2))


  def matchTest(x: Any): Any = x match {
    case 1 => "one"
    case "two" => 2
    case y: Int => "scala.Int"
  }

  println(matchTest(5))


  def apply(f: Any => String, v: Int) = f(v)

  val decorator = new Decorator("[", "]")
  println(apply(decorator.layout, 7))
  println("Factorial of 4: " + factorial(4))

  def factorial(x: Int): Int = {
    def fact(x: Int, accumulator: Int): Int = {
      if (x <= 1) accumulator
      else fact(x - 1, x * accumulator)
    }

    fact(x, 1)


  }
}

class Decorator(left: String, right: String) {
  def layout[A](x: A) = left + x.toString() + right
}