package net.pierreandrews.utils

/**
 * An Iterator that takes in a set of partially sorted inputs and merges them in a sorted manned, in descending order.
 *
 * If the inputs are not partially sorted, than the output will not be sorted properly. This sorter more or less
 * only needs inputs.size*size(I) memory as it lazily loads each line from the inputs.
 *
 * The second parameter is a function that extracts the field to be sorted on. It extracts it in an Option as the extraction
 * might fail (i.e. we can't parse the date out of the logline)
 *
 * User: pierre
 * Date: 11/30/14
 */
class LineSorter[I, T: Ordering](inputs: Seq[Iterator[I]], extractOrderField: I => Option[T]) extends Iterator[I] {

  //filter out the log lines to only keep the ones
  // which are parseable.
  // This flatMap is done on an iterator, so it's executed lazily only when the line is pulled.
  private val bufferedInputs = inputs.map(_.flatMap { l=>
    //get rid of the lines we can't parse
    extractOrderField(l).map { (_, l) }
  }.buffered)
  // we buffer the iterator so that we can peak at the top of the iterator without moving it forward.

  /**
  *  there are more lines in this iterator if at least one of the underlying iterators has more lines
   */
  override def hasNext: Boolean = {
    bufferedInputs.exists(_.hasNext)
  }

  /**
   * extract the next line, choosing the one with the maximum sorting field from all the available iterators
   * returns the maximum next line
   */
  override def next(): I = {
    // only get the possible next line from the iterators that still have content available
    // we will then figure out which one to return
    val possibleNext = bufferedInputs.collect {
      case it if it.hasNext =>
        (it.head, it)
    }

    //find out the maximum ordering field from the possible next lines
    // maxBy will sort the iterator in descending order.
    val ((sortField, maxNext), itToAdvance) = possibleNext.maxBy {
      case ((headOrder, _), _) =>
        headOrder
    }

    // move the chosen iterator forward
    itToAdvance.next()
    // return the line from that iterator
    maxNext
  }
}
