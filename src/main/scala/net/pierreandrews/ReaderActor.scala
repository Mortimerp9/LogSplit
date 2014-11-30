package net.pierreandrews

import java.io.{ File, FilenameFilter }

import akka.event.LoggingReceive

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

import akka.actor.{ Actor, ActorRef, RootActorPath }
import akka.cluster.{ Cluster, Member, MemberStatus }
import akka.cluster.ClusterEvent.{ CurrentClusterState, MemberUp }
import net.pierreandrews.Protocol._

import scala.util.control.NonFatal

/**
 * TODO DOC
 * User: pierre
 * Date: 11/28/14
 */
class ReaderActor(args: LogSplitAppArgs, input: File, partIdx: Int) extends Actor {

  val cluster = Cluster(context.system)

  val writers: ListBuffer[ActorRef] = ListBuffer()

  /**
   * we keep a buffer of lines in advance for each server to avoid being blocked by one single slow node
   */
  val logsToDeliver: IndexedSeq[mutable.Queue[LogLine]] = for {
    i <- 0 until args.numServers
  } yield {
    mutable.Queue[LogLine]()
  }

  //use a BufferedIterator to look at the head of the iterator
  // without discarding it (i.e. there is always one line in memory)
  val lines: BufferedIterator[Option[LogLine]] = lineIterator()

  // subscribe to cluster changes, MemberUp
  // re-subscribe when restart
  override def preStart(): Unit = {
    println(s"starting reader ${args.serverID}.$partIdx on file ${input.getAbsolutePath}")
    cluster.subscribe(self, classOf[MemberUp])
    fillUpQueue()
  }

  override def postStop(): Unit = cluster.unsubscribe(self)

  override def receive: Receive = LoggingReceive {
    ///////////////// cluster management
    case state: CurrentClusterState =>
      state.members.filter(_.status == MemberStatus.Up) foreach register
    case MemberUp(m) => register(m)
    case RegisterWriter(id) =>
      writers.prepend(sender())
      println(s"Writer $id registering on ${args.serverID}")

    ////////////////// log distribution logic
    case msg @ RequestLog(requestServerId) if requestServerId < args.numServers =>
      val partitionQueue = logsToDeliver(requestServerId)
      try {
        val log = partitionQueue.dequeue()
        sender() ! WriteLog(args.serverID, log, partIdx)
        fillUpQueue()
      } catch {
        case NonFatal(e) =>
          if (!lines.hasNext) {
            sender() ! LogDone(args.serverID)
            checkEnd()
          } else {
            //fill up the queue
            fillUpQueue()
          }
      }
  }

  def lineIterator(): BufferedIterator[Option[LogLine]] = {
    //use an iterator to lazily load the lines
    // make it buffered so we can peak at the head of the iterator
    Source.fromFile(input).getLines().map(LogLine(_, args.numServers)).buffered
  }

  /**
   * fill up the queues until the next line is of a partition whose queue is full
   */
  @tailrec
  private final def fillUpQueue(): Unit = {
    if (lines.hasNext) {
      lines.head match {
        case Some(log) =>
          val partitionQueue = logsToDeliver(log.partition)
          if (partitionQueue.size < args.maxReadBuffer) {
            partitionQueue.enqueue(log)
            lines.next()
            writers.foreach(_ ! LogAvailable) // TODO only send to writers interested in this partition
            fillUpQueue()
          }
        case None =>
          lines.next()
          fillUpQueue()
         //TODO it's a bad line, maybe log error
      }
    }
  }

  def register(member: Member): Unit = {
    if (member.hasRole("logsplit")) {
      context.actorSelection(RootActorPath(member.address) / "user" / "writer") ! RegisterReader(args.serverID, partIdx)
    }
  }

  //we are at the end of the read when the lines are done and all
  // queues have drained
  def checkEnd(): Unit = {
    if (!lines.hasNext && logsToDeliver.forall(_.isEmpty)) {
      // we are done, send a message to all writers and stop this actor
      writers.foreach(_ ! LogDone(args.serverID))
      context.stop(self)
    }
  }
}

