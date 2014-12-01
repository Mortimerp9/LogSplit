package net.pierreandrews

import java.io.{ File, PrintWriter }

import akka.actor.{ ActorLogging, Props, Actor }
import akka.event.LoggingReceive
import net.pierreandrews.Protocol.{ FileNameData, SortUser, GiveMeWork, StartSorting }
import net.pierreandrews.SorterActor.SorterWorkerActor
import net.pierreandrews.utils.LineSorter
import org.joda.time.{ DateTimeComparator, DateTime }

import scala.io.Source
import scala.util.control.NonFatal

/**
 * Manages the sorting of the partial files for each userid.
 * Once the log distribution is done, we are left with a number of part files coming from each server
 * each part file is sorted but all the parts are not globally sorted. Once we are done collecting logs,
 * we have to sort the lines again.
 *
 * This is the manager and will start a number of workers and distribute the workload to them with a pull logic.
 *   Each worker starts sorting a set of files for one user; when it's done, it asks for more work to the manager.
 *   The pull pattern allows for distributing more work to the fastest sorter and balancing work better.
 *
 * User: pierre
 * Date: 11/28/14
 */
class SorterActor(args: LogSplitAppArgs) extends Actor with ActorLogging {
  // file pattern to read logs from. This are the part output of the WriterActor (as defined in FileCache)
  private final val ValidPartFile = "(.*).([0-9]+).([0-9]+).part$".r
  // the files to process grouped by users
  var fileGroups: Iterator[(String, Seq[FileNameData])] = Iterator()
  // the number of active children. When this reaches 0, we are done sorting everything.
  var childCount = args.numSortWorkers

  override def receive: Receive = LoggingReceive {
    case StartSorting =>
      //the log distribution is done, we know have all the logs for a set of users
      // on this node and want to sort the part files together in one file. Start the workers and
      // distribute work
      log.info("START SORTING")
      // load the list of log parts
      val files: Seq[FileNameData] = Option(args.output.listFiles)
        .map(_.toSeq)
        .getOrElse(Seq())
        .flatMap { file =>
          file.getName match {
              //only keep the ones that match our pattern and extract dome data at the same time.
            case ValidPartFile(userid, serverId, partId) => Some(FileNameData(userid, serverId.toInt, partId.toInt, file))
            case _ => None
          }
        }

      //group the part files per user, we need to merge these
      fileGroups = files.groupBy(_.userId).toIterator

      //Start Workers
      for (i <- 0 until args.numSortWorkers) {
        context.actorOf(Props(new SorterWorkerActor(args)))
      }

    case GiveMeWork =>
      //give work to available worker
      if (fileGroups.hasNext) {
        //get the next set of files to merge and send them to the worker
        val (userid, files) = fileGroups.next()
        sender() ! SortUser(userid, files)
      } else {
        //no more work, stop that worker
        context.stop(sender())
        childCount -= 1
        if (childCount <= 0) {
          //if we don't have anymore work and all sort workers are done, we are DONE!, shutdown the system
          log.info("DONE SORTING")
          context.system.shutdown()
        }
      }
  }
}

object SorterActor {

  //define an ordering for DateTime, found on stackoverflow
  implicit object JodaDateTimeOrdering extends Ordering[DateTime] {
    val dtComparer = DateTimeComparator.getInstance()

    def compare(x: DateTime, y: DateTime): Int = {
      dtComparer.compare(x, y)
    }
  }

  /**
   * the sorting worker. Each actor runs in parallel and will merge a set of files for one
   * specific user before asking for more work from the manager.
   */
  class SorterWorkerActor(args: LogSplitAppArgs) extends Actor with ActorLogging {

    //first of all, ask for work when we are ready
    override def preStart(): Unit = {
      //pull work from parent when ready
      context.parent ! GiveMeWork
    }

    override def receive: Actor.Receive = {
      //receive workload
      case SortUser(userid, files) =>
        log.info("sorting for {}", userid)
        //part files are already ordered for each server, just append them in order of partID
        val serverParts = files.groupBy(_.serverId).values.toSeq.map {
          parts => parts.sortBy(_.partId).toIterator.flatMap(f => Source.fromFile(f.file).getLines)
        }

        //server inputs are sorted, but not relative to other server parts
        // use the LineSorter iterator to merge them and get the lines in order.
        val inputIterator: Iterator[String] = new LineSorter(serverParts, Parser.extractDate)

        //all the sorting work is done by the iterator, we just need to print them out
        val writer = new PrintWriter(new File(args.output, userid))
        var
        try {
          inputIterator.foreach(writer.println)
          //maybe delete the part files (do not delete if there was an exception)
          if (args.deletePartFiles) {
            files.foreach {
              f =>
                f.file.delete()
            }
          }
          log.info("done sorting for {}", userid)
        } catch {
          case NonFatal(e) =>
            //something wrong happened, just give up on that user and try another one
            log.error(e, "failed sorting for {}", userid)
        } finally {
          //close the writer
          writer.close()
          // get more work now that we are done.
          context.parent ! GiveMeWork
        }
    }
  }
}
