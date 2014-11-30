package net.pierreandrews

import java.text.SimpleDateFormat
import java.util.{Locale, Date}

import scala.util.Try
import scala.util.hashing.MurmurHash3

object LogLine {
  private final val dateformat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z",Locale.ENGLISH)
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
        val userIdx = line.indexOfSlice(""""userid=""")+8
        if (userIdx < 8) {
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

case class LogLine(userid: String, date: Date, line: String, numServers: Int) {
  /**
   * use murmurhash3 to generate a "good" distribution over all servers
   */
  lazy val partition = Math.abs(MurmurHash3.stringHash(userid) % numServers)
}
