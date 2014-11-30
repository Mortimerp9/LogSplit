package net.pierreandrews.utils

import java.util.Date

import org.joda.time.DateTime
import org.scalatest.{ Matchers, FunSuite }

import scala.util.Random

/**
 * TODO DOC
 * User: pierre
 * Date: 11/30/14
 */
class LineSorterTest extends FunSuite with Matchers {

  test("should sort") {

    val numPerIt = 100

    def sortedSeq(): Seq[Int] = Iterator.iterate(numPerIt) { prev =>
      val dec = Random.nextInt(prev)
      prev - dec
    }.take(numPerIt).toSeq

    val it1 = sortedSeq()
    val it2 = sortedSeq()
    val it3 = sortedSeq()

    val expected = (it1 ++ it2 ++ it3).sortBy(x => -x)

    val it: Seq[Int] = new LineSorter(Seq(it1.iterator, it2.iterator, it3.iterator), (x: Int) => Some(x)).toSeq

    it.toSeq should equal(expected)

  }

  test("should sort dates") {
    import net.pierreandrews.SorterActor.JodaDateTimeOrdering

    val numPerIt = 5
    def sortedSeq(): Seq[DateTime] = Iterator.iterate(System.currentTimeMillis) { previousTS =>
      val dec = Random.nextInt(3600) * 1000L
      previousTS - dec
    }.map { ts =>
      new DateTime(ts)
    }.take(5).toSeq

    val it1 = sortedSeq()
    val it2 = sortedSeq()
    val it3 = sortedSeq()

    val expected = (it1 ++ it2 ++ it3).sortBy(x => x).reverse
    val it: Seq[DateTime] = new LineSorter(Seq(it1.iterator, it2.iterator, it3.iterator), (x: DateTime) => Some(x)).toSeq

    it.toSeq should equal(expected)

  }

  test("should sort with illegal lines") {

    val numPerIt = 100

    def sortedSeq(): Seq[Int] = Iterator.iterate(numPerIt) { prev =>
      val dec = Random.nextInt(prev)
      prev - dec
    }.take(numPerIt).toSeq

    val it1 = sortedSeq()
    val it2 = sortedSeq()
    val it3 = sortedSeq()

    val expected = (it1 ++ it2 ++ it3).filter(_%4!=0).sortBy(x => -x)

    def sortWithFail(x: Int) = {
      if(x % 4 == 0) None
      else Some(x)
    }

    val it: Seq[Int] = new LineSorter(Seq(it1.iterator, it2.iterator, it3.iterator), sortWithFail).toSeq

    it.toSeq should equal(expected)
  }

}
