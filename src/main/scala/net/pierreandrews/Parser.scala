package net.pierreandrews

import java.util.Locale

import scala.util.Try
import scala.util.hashing.MurmurHash3

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

object Parser {
  //parse the log lines. Naive implementation

  //use joda DateTime as java SimpleDateFormat is unreliable
  // assume English log dates of the form: 13/Oct/2014:08:06:37 -0700
  private final val dateformat = DateTimeFormat.forPattern("dd/MMM/yyyy:HH:mm:ss Z").withLocale(Locale.ENGLISH)

  // extract a date, might fail and return None
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

  // extract a userid, might fail and return None
  // we expect user ids to be between userid=" and a closing "
  // we do not assume much about the format of the id itself.
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

  // assign a userid to a partition (server node) where it's log line will be gathered.
  def partition(userid: String, numServers: Int) = {
    //murmur hash is faster than cryptographic hashes and has a good distribution
    // so log lines will be evenly distributed to server.
    // The design of this splitter system assumes that userids are distributed evenly between each server,
    // using the murmur hash means that around a third of each log lines will be send over to another node (if we
    // have three servers). If the users are not balanced (all the logs for one users are always on one node), then this
    // will generate more network IO than really needed, if this is the case, we should adopt another strategy (see README)
    Math.abs(MurmurHash3.stringHash(userid) % numServers)
  }

}
