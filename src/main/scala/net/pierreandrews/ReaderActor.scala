package net.pierreandrews

import java.io.{File, FilenameFilter}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.io.Source

import akka.actor.{Actor, ActorRef, RootActorPath}
import akka.cluster.{Cluster, Member, MemberStatus}
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberUp}
import net.pierreandrews.Protocol._

import scala.util.control.NonFatal

/**
 * TODO DOC
 * User: pierre
 * Date: 11/28/14
 */
class ReaderActor(args: LogSplitAppArgs) extends Actor {

  val cluster = Cluster(context.system)

  val writers: Array[ActorRef] = new Array(args.numServers)
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
  val lines: BufferedIterator[Option[LogLine]] = lineIterator

  // subscribe to cluster changes, MemberUp
  // re-subscribe when restart
  override def preStart(): Unit = {
    cluster.subscribe(self, classOf[MemberUp])
    fillUpQueue()
  }
  override def postStop(): Unit = cluster.unsubscribe(self)

  override def receive: Receive = {
    ///////////////// cluster management
    case state: CurrentClusterState =>
      state.members.filter(_.status == MemberStatus.Up) foreach register
    case MemberUp(m) => register(m)
    case RegisterWriter(id) =>
      writers(id) = sender()
      println(s"Writer $id registering on ${args.serverID}")


    ////////////////// log distribution logic
    case msg@RequestLog(requestServerId) if requestServerId < args.numServers =>
      val partitionQueue = logsToDeliver(requestServerId)
      try {
        val log = partitionQueue.dequeue()
        sender() ! WriteLog(args.serverID, log)
        fillUpQueue()
      } catch {
        case NonFatal(e) =>
          if(!lines.hasNext) {
            sender() ! LogDone(args.serverID)
            checkEnd()
          } else {
            //fill up the queue and try again, we might be in an invalid state
            fillUpQueue()
            self ! msg
          }
      }
  }

  def lineIterator : BufferedIterator[Option[LogLine]] = {
    //listFiles might return null, wrap in Option
    val files: Seq[File] = Option(args.input.listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = return name.endsWith(".log")
    })).map(_.toSeq).getOrElse(Seq())
    //use an iterator to lazily load the lines
    files.iterator.flatMap { f =>
      Source.fromFile(f).getLines
    }.map(LogLine(_, args.numServers))
  }.buffered

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
            fillUpQueue()
          }
        case None => //TODO it's a bad line, maybe log error
      }
    }
  }

  def register(member: Member): Unit = {
    if (member.hasRole("logsplit")) {
      context.actorSelection(RootActorPath(member.address) / "user" / "writer") ! RegisterReader(args.serverID)
    }
  }

  //we are at the end of the read when the lines are done and all
  // queues have drained
  def checkEnd(): Unit = {
    if(!lines.hasNext && logsToDeliver.forall(_.isEmpty)) {
      // we are done, send a message to all writers and stop this actor
      writers.foreach(_ ! LogDone(args.serverID))
      context.stop(self)
    }
  }
}

