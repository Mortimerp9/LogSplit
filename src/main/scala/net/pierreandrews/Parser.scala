package net.pierreandrews

import java.util.Locale

import scala.util.Try
import scala.util.hashing.MurmurHash3

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

object Parser {

  private final val dateformat = DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss Z").withLocale(Locale.ENGLISH)

  def extractDate(line: String): Option[DateTime] = {
    val dateIdx = line.indexOf('[')
    if (dateIdx < 0) {
      None
    } else {
      val endIdx = line.indexOf(']')
      if (endIdx < 0) {
        None
      } else {
        val date = line.substring(dateIdx + 1, endIdx)
        Try(dateformat.parseDateTime(date)).toOption
      }
    }
  }

  def extractUser(line: String): Option[String] = {
    val userIdx = line.indexOfSlice(""""userid=""") + 8
    if (userIdx < 8) {
      None
    } else {
      val userEnd = line.indexOf('"', userIdx)
      if (userEnd > 0) {
        Some(line.substring(userIdx, userEnd))
      } else {
        None
      }
    }
  }

  def partition(userid: String, numServers: Int) = Math.abs(MurmurHash3.stringHash(userid) % numServers)

}
