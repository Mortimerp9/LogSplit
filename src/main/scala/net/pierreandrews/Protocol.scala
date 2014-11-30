package net.pierreandrews

import java.io.File

/**
 * TODO DOC
 * User: pierre
 * Date: 11/28/14
 */
object Protocol {

  case class IdAndLine(line: String) {
    val userid: Option[String] = Parser.extractUser(line)
    def partition(numServers: Int): Option[Int] = userid.map(Parser.partition(_, numServers))
  }

  case class RegisterWriter(serverId: Int)
  case class RegisterReader(serverId: Int, partIdx: Int, totalReaders: Int)

  case class RequestLog(serverId: Int)
  case class WriteLog(readerId: Int, logLine: IdAndLine, partIdx: Int)
  case class LogDone(readerId: Int, partIdx: Int)


  case object StartReading

  case object LogAvailable


  case class FileNameData(userId: String, serverId: Int, partId: Int, file: File)
  case object StartSorting
  case object GiveMeWork
  case class SortUser(userId: String, files: Seq[FileNameData])

}
