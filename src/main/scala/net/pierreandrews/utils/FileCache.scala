package net.pierreandrews.utils

import java.io.{FileWriter, PrintWriter, File}

import net.pierreandrews.{LogLine, LogSplitAppArgs}

/**
 * A LRU of PrintWriters that opens/close the files when needed
 *  and tries to minimize the handle rotations.
 * User: pierre
 * Date: 11/28/14
 */

class FileCache(args: LogSplitAppArgs) {

  val lruCache = new LRUOfFiles(args.maxWriteOpen)

  //we write lines in a file per reader
  // this is because each reader has log lines sorted, but we don't know
  // how the partial order is organized between servers. We can only guarantee the order for the same reader
  // once we have all the log lines, we can start a merging step that will merge all the partially ordered files
  def write(log: LogLine, readerId: Int, partId: Int): Unit = {
    val filename = s"${log.userid}.$readerId.$partId"
    val writer = Option(lruCache.get(filename)).getOrElse {
      val file = new File(args.output, filename)
      val newWriter = new PrintWriter(new FileWriter(file, true)) //true to append to a possibly existing file
      lruCache.put(log.userid, newWriter)
      newWriter
    }
    writer.println(log.line)
  }

  def close(): Unit = {
    lruCache.closeAll()
  }

}
