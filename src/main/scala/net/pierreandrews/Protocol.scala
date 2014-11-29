package net.pierreandrews

/**
 * TODO DOC
 * User: pierre
 * Date: 11/28/14
 */
object Protocol {

  case class RegisterWriter(serverId: Int)
  case class RegisterReader(serverId: Int)

  case class RequestLog(serverId: Int)
  case class WriteLog(readerId: Int, logLine: LogLine)
  case class LogDone(readerId: Int)

  case object StartSorting

}
