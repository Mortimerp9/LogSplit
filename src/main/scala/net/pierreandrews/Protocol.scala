package net.pierreandrews

/**
 * TODO DOC
 * User: pierre
 * Date: 11/28/14
 */
object Protocol {

  case class RegisterWriter(serverId: Int)
  case class RegisterReader(serverId: Int, partIdx: Int)

  case class RequestLog(serverId: Int)
  case class WriteLog(readerId: Int, logLine: LogLine, partIdx: Int)
  case class LogDone(readerId: Int)

  case object StartSorting

  case object LogAvailable

}
