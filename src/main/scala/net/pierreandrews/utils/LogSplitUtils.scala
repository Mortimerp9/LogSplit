package net.pierreandrews.utils

/**
 * TODO DOC
 * User: pierre
 * Date: 11/30/14
 */
object LogSplitUtils {

  //spread files over readers, from: http://stackoverflow.com/a/11456797/618089
  def cut[A](xs: Seq[A], n: Int) = {
    val (quot, rem) = (xs.size / n, xs.size % n)
    val (smaller, bigger) = xs.splitAt(xs.size - rem * (quot + 1))
    smaller.grouped(quot) ++ bigger.grouped(quot + 1)
  }

}
