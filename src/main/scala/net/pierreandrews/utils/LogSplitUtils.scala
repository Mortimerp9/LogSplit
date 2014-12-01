package net.pierreandrews.utils

import java.io.File

import net.pierreandrews.Protocol.FileNameData

import scala.annotation.tailrec
import scala.io.Source

object LogSplitUtils {

  /**
   * Cut an indexed sequence in sub-sequences of more or less size n. This is
   * used to spread files over readers.
   * from: http://stackoverflow.com/a/11456797/618089
   */
  def cut[A](xs: Seq[A], n: Int): Iterator[Seq[A]] = {
    if (n > xs.size) {
      xs.toIterator.map(x => Seq(x))
    } else {
      val (quot, rem) = (xs.size / n, xs.size % n)
      val (smaller, bigger) = xs.splitAt(xs.size - rem * (quot + 1))
      smaller.grouped(quot) ++ bigger.grouped(quot + 1)
    }
  }

  class JoinIterators(fileList: Seq[File]) extends Iterator[String] {

    val files = fileList.toIterator
    var currentIterator: Option[Iterator[String]] = None
    var fileHandle: Option[Source] = None

    override def hasNext: Boolean = currentIterator.forall(_.hasNext) || files.hasNext

    override def next(): String = {
      @tailrec
      def nextRec(): String = {
        currentIterator match {
          case None if files.hasNext =>
            fileHandle = Some(Source.fromFile(files.next()))
            currentIterator = fileHandle.map(_.getLines)
            nextRec()
          case None if !files.hasNext =>
            null
          case Some(it) if it.hasNext =>
            it.next()
          case Some(it) if !it.hasNext && files.hasNext =>
            fileHandle.foreach(_.close())
            fileHandle = Some(Source.fromFile(files.next()))
            currentIterator = fileHandle.map(_.getLines)
            nextRec()
          case Some(it) if !it.hasNext && !files.hasNext =>
            fileHandle.foreach(_.close())
            null
          case _ => null

        }
      }
      nextRec()
    }
  }

}
