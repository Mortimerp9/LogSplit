package net.pierreandrews

import java.io.File

/**
 * Defines all the messages that are exchanged between the actors
 * User: pierre
 * Date: 11/28/14
 */
object Protocol {

  // a log line with simple extraction features
  case class IdAndLine(line: String) {
    val userid: Option[String] = Parser.extractUser(line)
    def partition(numServers: Int): Option[Int] = userid.map(Parser.partition(_, numServers))
  }

  //Registering messages
  case class RegisterWriter(serverId: Int)
  case class RegisterReader(serverId: Int, partIdx: Int, totalReaders: Int)

  //Log exchange messages
  /**
   * a writer worker is requesting a log line for this partition
   */
  case class RequestLog(serverId: Int)

  /**
   * a reader is sending some work (in response to RequestLog) to a writer
   */
  case class WriteLog(readerId: Int, logLine: IdAndLine, partIdx: Int)

  /**
   * a reader is telling an idle writer that work is available for its partition
   */
  case object LogAvailable

  /**
   * A reader has read through all it's files.
   */
  case class LogDone(readerId: Int, partIdx: Int)


  /**
   * the cluster is setup, tell the writers to start pulling work from the readers
   */
  case object StartReading

  //sorting/merging messages

  /**
   * data extracted from a filename for merging
   */
  case class FileNameData(userId: String, serverId: Int, partId: Int, file: File)

  /**
   * we are done moving log lines around, start merging the local files
   */
  case object StartSorting

  /**
   * a sort worker wants work to merge
   */
  case object GiveMeWork

  /**
   * give work to a worker
   */
  case class SortUser(userId: String, files: Seq[FileNameData])

}
