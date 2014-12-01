package net.pierreandrews.utils

import java.io.{ File, FileWriter, PrintWriter }

import net.pierreandrews.LogSplitAppArgs
import net.pierreandrews.Protocol.IdAndLine

/**
 * A LRU of PrintWriters that opens/close the files when needed
 *  and tries to minimize the handle rotations.
 *
 *  We cannot predict the number of users that will be in the logs or when we'll have processed all the logs for
 *  a particular user. So we do not want to keep handles open for ALL the users nor do we want to keep
 *  reopening handles.
 *
 *  The Least Recently Used (LRU) strategy keeps the most often used file handles open and closes the other ones.
 *
 * User: pierre
 * Date: 11/28/14
 */
class FileCache(args: LogSplitAppArgs) {

  /*
   * we need to override the apache LRU to close the handles when they get expelled from the cache.
   *  This bugs in scala, so the override is done in java. See LRUOfFiles.java
   */
  val lruCache = new LRUOfFiles(args.maxWriteOpen)

  //we write lines in a file per user, per reader;
  // this is because each reader has log lines sorted, but we don't know
  // how the partial order is organized between servers. We can only guarantee the order for the same reader
  // once we have all the log lines, we can start a merging step that will merge all the partially ordered files (see SorterActor)
  def write(log: IdAndLine, readerId: Int, partId: Int): Unit = {
    log.userid.foreach { userid =>
      //if we could parse the userid from the log line, get the file handle for that user/node/part.
      val filename = s"${userid}.$readerId.$partId.part"
      val writer = Option(lruCache.get(filename)).getOrElse {
        //if the cache doesn't contain the handle
        // create a new one
        val file = new File(args.output, filename)
        val newWriter = new PrintWriter(new FileWriter(file, true)) //true to append to a possibly existing file
        //and add it to the cache
        lruCache.put(filename, newWriter)
        newWriter
      }
      writer.println(log.line)
    }
  }

  def close(): Unit = {
    lruCache.closeAll()
  }

}
