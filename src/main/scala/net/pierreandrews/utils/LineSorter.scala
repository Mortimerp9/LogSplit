package net.pierreandrews.utils

/**
 * TODO DOC
 * User: pierre
 * Date: 11/30/14
 */
class LineSorter[I, T: Ordering](inputs: Seq[Iterator[I]], extractOrderField: I => Option[T]) extends Iterator[I] {

  val bufferedInputs = inputs.map(_.flatMap { l=>
    //get rid of the lines we can't parse
    extractOrderField(l).map { (_, l) }
  }.buffered)

  override def hasNext: Boolean = {
    bufferedInputs.exists(_.hasNext)
  }

  override def next(): I = {
    val possibleNext = bufferedInputs.collect {
      case it if it.hasNext =>
        (it.head, it)
    }

    val ((sortField, maxNext), itToAdvance) = possibleNext.maxBy {
      case ((headOrder, _), _) =>
        headOrder
    }

    itToAdvance.next()
  //  s"$sortField\t$maxNext"
    maxNext
  }
}
