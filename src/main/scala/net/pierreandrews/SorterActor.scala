package net.pierreandrews

import java.io.{ File, PrintWriter }

import akka.actor.{ Props, Actor }
import akka.event.LoggingReceive
import net.pierreandrews.Protocol.{ FileNameData, SortUser, GiveMeWork, StartSorting }
import net.pierreandrews.SorterActor.SorterWorkerActor
import net.pierreandrews.utils.LineSorter
import org.joda.time.{DateTimeComparator, DateTime}

import scala.io.Source
import scala.util.control.NonFatal

/**
 * Manages the sorting of the partial files for each userid
 * User: pierre
 * Date: 11/28/14
 */
class SorterActor(args: LogSplitAppArgs) extends Actor {
  private final val ValidPartFile = "(.*).([0-9]+).([0-9]+).part$".r
  var fileGroups: Iterator[(String, Seq[FileNameData])] = Iterator()
  var childCount = args.numSortWorkers

  override def receive: Receive = LoggingReceive {
    case StartSorting =>
      println("START SORTING")
      val files: Seq[FileNameData] = Option(args.output.listFiles)
        .map(_.toSeq)
        .getOrElse(Seq())
        .flatMap { file =>
          file.getName match {
            case ValidPartFile(userid, serverId, partId) => Some(FileNameData(userid, serverId.toInt, partId.toInt, file))
            case _ => None
          }
        }

      fileGroups = files.groupBy(_.userId).toIterator

      //Start Workers
      for (i <- 0 until args.numSortWorkers) {
        context.actorOf(Props(new SorterWorkerActor(args)))
      }

    case GiveMeWork =>
      //give work to available worker
      if (fileGroups.hasNext) {
        val (userid, files) = fileGroups.next()
        sender() ! SortUser(userid, files)
      } else {
        context.stop(sender())
        childCount -= 1
        if (childCount <= 0) {
          println("DONE SORTING")
          //TODO shutdown
        }
      }
  }
}

object SorterActor {


  implicit object JodaDateTimeOrdering extends Ordering[org.joda.time.DateTime] {
    val dtComparer = DateTimeComparator.getInstance()

    def compare(x: DateTime, y: DateTime): Int = {
      dtComparer.compare(x, y)
    }
  }

  class SorterWorkerActor(args: LogSplitAppArgs) extends Actor {

    override def preStart(): Unit = {
      //pull work from parent when ready
      context.parent ! GiveMeWork
    }

    override def receive: Actor.Receive = {
      //receive workload
      case SortUser(userid, files) =>
        println(s"sorting for $userid")
        //part files are already ordered for each server, just append them in order of partID
        val serverParts = files.map(f =>
          Source.fromFile(f.file).getLines
        )
        /*files.groupBy(_.serverId).values.toSeq.map {
          parts => parts.sortBy(_.partId).toIterator.flatMap(f => Source.fromFile(f.file).getLines)
        } */

        //server inputs are sorted, but not relative to other server part
        // get the lines in order
        val inputIterator: Iterator[String] = new LineSorter(serverParts, Parser.extractDate)

        //and print them out
        val writer = new PrintWriter(new File(args.output, userid))
        try {
          inputIterator.foreach(writer.println)
          println(s"done sorting for $userid")
        } catch {
          case NonFatal(e) =>
            println(s"failed sorting for $userid")
          //TODO ???
        } finally {
          writer.close()
          //TODO delete files?
          context.parent ! GiveMeWork
        }
    }
  }
}
