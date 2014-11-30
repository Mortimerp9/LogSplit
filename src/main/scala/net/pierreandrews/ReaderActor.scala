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
class ReaderActor(args: LogSplitAppArgs, inputFiles: Seq[File], partIdx: Int) extends Actor {

  val cluster = Cluster(context.system)

  /**
   * keep registered writers grouped by the server id where they are
   */
  val writers: Array[ActorRef] = new Array(args.numServers)
  val blockedWriters: Array[Boolean] = new Array(args.numServers)
  var registeredWriters = 0

  /**
   * we keep a buffer of lines in advance for each server to avoid being blocked by one single slow node
   */
  val logsToDeliver: IndexedSeq[mutable.Queue[IdAndLine]] = for {
    i <- 0 until args.numServers
  } yield {
    mutable.Queue[IdAndLine]()
  }

  //use a BufferedIterator to look at the head of the iterator
  // without discarding it (i.e. there is always one line in memory)
  val lines: BufferedIterator[IdAndLine] = lineIterator()

  // subscribe to cluster changes, MemberUp
  // re-subscribe when restart
  override def preStart(): Unit = {
    println(s"starting reader ${args.serverID}.$partIdx on files ${inputFiles.map(_.getAbsolutePath).mkString(",")}")
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
      println(s"Writer $id registering on ${args.serverID}.${partIdx}")
      writers(id) = sender()
      registeredWriters+=1
      if(registeredWriters >= args.numServers) {
        println(s"Reader ${args.serverID}.$partIdx is becoming active")
        context.become(activeReader)
        notifyWriters()
      }

    case RequestLog(requestServerId) if requestServerId < args.numServers =>
       blockedWriters(requestServerId) = true
  }

  def activeReader: Receive = {
    ////////////////// log distribution logic
    case RequestLog(requestServerId) if requestServerId < args.numServers =>
      val partitionQueue = logsToDeliver(requestServerId)
      try {
        val log = partitionQueue.dequeue()
        sender() ! WriteLog(args.serverID, log, partIdx)
        fillUpQueue()
      } catch {
        case NonFatal(e) =>
          if (!lines.hasNext) {
            checkEnd()
          } else {
            //fill up the queue
            blockedWriters(requestServerId) = true
            fillUpQueue()
          }
      }
  }

  def lineIterator(): BufferedIterator[IdAndLine] = {
    //use an iterator to lazily load the lines
    // make it buffered so we can peak at the head of the iterator
    inputFiles.toIterator.flatMap(f => Source.fromFile(f).getLines().map(IdAndLine)).buffered
  }


  def notifyWriters(): Unit = {
    //notify the writers that some logs have been made available
    for (i <- 0 until logsToDeliver.size) {
      //but only for non empty queues
      if (logsToDeliver(i).size > 0 && blockedWriters(i)) {
        blockedWriters(i) = false
        writers(i) ! LogAvailable
      }
    }
  }
  /**
   * fill up the queues until the next line is of a partition whose queue is full
   */
  private final def fillUpQueue(): Unit = {
    @tailrec
    def recurseFillUp() {
      if (lines.hasNext) {
        val log = lines.head
        log.partition(args.numServers) match {
          case Some(partition) =>
            //valid line, find the right queue and push it
            val partitionQueue = logsToDeliver(partition)
            if (partitionQueue.size < args.maxReadBuffer) {
              //but only if the buffer is not full, otherwise block
              partitionQueue.enqueue(log)
              lines.next()
              recurseFillUp()
            }
          case None =>
            //invalid line, cannot be parsed, drop it
            //TODO log bad line
            lines.next()
            recurseFillUp()
        }
      }
    }

    recurseFillUp()
    notifyWriters()
  }

  def register(member: Member): Unit = {
    if (member.hasRole("logsplit")) {
      context.actorSelection(RootActorPath(member.address) / "user" / "writer") ! RegisterReader(args.serverID, partIdx, args.numReaderWorkers)
    }
  }

  //we are at the end of the read when the lines are done and all
  // queues have drained
  def checkEnd(): Unit = {
    if (!lines.hasNext && logsToDeliver.forall(_.isEmpty)) {
      println(s"Reader ${args.serverID}.$partIdx is done")
      // we are done, send a message to all writers and stop this actor
      writers.foreach(_ ! LogDone(args.serverID, partIdx))
      context.stop(self)
    }
  }
}

