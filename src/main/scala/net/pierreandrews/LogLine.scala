package net.pierreandrews

import java.text.SimpleDateFormat
import java.util.Date

import scala.util.Try
import scala.util.hashing.MurmurHash3

object LogLine {
  private final val dateformat = new SimpleDateFormat("dd/MMM/YYYY:HH:mm:ss Z")
  def apply(line: String, numServers: Int): Option[LogLine] = {

    val dateIdx = line.indexOf('[')
    if (dateIdx < 0) {
      None
    } else {
      val endIdx = line.indexOf(']')
      if (endIdx < 0) {
        None
      } else {
        val date = line.substring(dateIdx + 1, endIdx)
        val userIdx = line.indexOfSlice(""""userid=""")
        if (userIdx < 0) {
          None
        } else {
          val userEnd = line.indexOf('"', userIdx)
          val userid = line.substring(userIdx, userEnd)
          Try(dateformat.parse(date)).toOption.map {
            parsedDate =>
              new LogLine(userid, parsedDate, line, numServers)
          }
        }
      }
    }
  }
}

class LogLine(val userid: String, val date: Date, val line: String, numServers: Int) {
  /**
   * use murmurhash3 to generate a "good" distribution over all servers
   */
  lazy val partition = Math.abs(MurmurHash3.stringHash(userid) % numServers)
  override def equals(l: Any): Boolean = {
    l match {
      case ll: LogLine => (ll.line == line)
      case _ => false
    }
  }
}
