package net.pierreandrews

import java.io.{ File, FilenameFilter }

import akka.event.LoggingReceive
import net.pierreandrews.utils.LogSplitUtils.JoinIterators

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source

import akka.actor.{ ActorLogging, Actor, ActorRef, RootActorPath }
import akka.cluster.{ Cluster, Member, MemberStatus }
import akka.cluster.ClusterEvent.{ MemberRemoved, CurrentClusterState, MemberUp }
import net.pierreandrews.Protocol._

import scala.util.control.NonFatal

/**
 * A reader actor is responsible for reading logs from the local filesystem and distributing them to the writer nodes.
 *
 * Each reader contacts the cluster and registers on ALL nodes. The nodes are responsible for assigning a writer to the
 * reader. Each reader communicates with one and only one worker on each node.
 *
 * A reader is responsible for reading all the lines from a set of consecutive buckets, it will append inputFiles in order
 * and lazily read from that iterator.
 *
 * Each reader works in parallel on a different set of local files. A reader doesn't know about other readers and works on files
 * that are only assigned to it, so we have no concurrency issues accessing files.
 *
 * Because lines are in order in the local files processed by a reader, it offers them in order to the writers.
 *
 * Not all writers might be executing at the same speed (one could be on another node that is slower, or has a slower IO), but lines
 * in the files are sequential an one line can only go to one worker.
 * If the line at the top of the file is assigned to a worker that is blocked or unreachable, the reader (and all its assigned writers) will
 * be blocked too.
 * To avoid being bound by the slowest node/IO in the cluster, each reader keeps a buffer of next lines so that it can advance in the file even
 * if one worker is blocked for a bit and distribute lines to other writers. If users are randomly distributed in the log files, and because we used
 * a balanced partitioning (see Parser.partition), then this works well. Increasing the buffer size will require more memory but will allow for
 * a faster processing.
 *
 * Writers send a RequestLog message to all their readers, they will then be idle until they receive a log line or a message to ping the readers again.
 * It's up to the reader to send this message in a timely manner or the cluster will be blocked. We do not send log lines proactively (push) to the
 * writers as we do not want to overflow their message queues and memory.
 *
 * User: pierre
 * Date: 11/28/14
 */
class ReaderActor(args: LogSplitAppArgs, inputFiles: Seq[File], partIdx: Int, totalReaders: Int) extends Actor with ActorLogging {

  // the akka cluster to communicate wiht
  val cluster = Cluster(context.system)

  // we have one writer actor per node. Array is indexed by server id
  val writers: Array[ActorRef] = new Array(args.numServers)
  // keep track of which writer asked for a logline that couldn't be delivered (because its queue was empty)
  // array is indexed by server id
  val blockedWriters: Array[Boolean] = new Array(args.numServers)
  //number of registered writers
  var registeredWriters = 0

  /**
   * we keep a buffer of lines in advance for each server to avoid being blocked by one single slow node.
   * The queues are not strictly bounded but we never push more than args.maxReadBuffer lines per partition (see fillUpQueue() ).
   *
   * The Seq is indexed per server id
   */
  val logsToDeliver: IndexedSeq[mutable.Queue[IdAndLine]] = for {
    i <- 0 until args.numServers
  } yield {
    mutable.Queue[IdAndLine]()
  }

  //use a BufferedIterator to look at the head of the iterator
  // without discarding it (i.e. there is always one line in memory+the ones in the queue)
  val lines: BufferedIterator[IdAndLine] = lineIterator()

  override def preStart(): Unit = {
    log.info("starting reader {}.{} on files {}", args.serverID, partIdx, inputFiles.map(_.getAbsolutePath).mkString(","))
    // subscribe to cluster changes
    cluster.subscribe(self, classOf[MemberUp])
    // fill up the queues in preparation for writers pulling work
    fillUpQueue()
  }
  // re-subscribe when restart
  override def postStop(): Unit = cluster.unsubscribe(self)

  // start in a cluster management mode. When all nodes are setup, move to an active mode where we
  // can send logs to writers
  override def receive: Receive = {
    ///////////////// cluster management
    case state: CurrentClusterState =>
      state.members.filter(_.status == MemberStatus.Up) foreach register
    case MemberUp(m) =>
      // a new node joins, go register on it
      register(m)

    //connect to writers that we are assigned to
    case RegisterWriter(id) =>
      log.info(s"Writer {} registering on {}.{}", id, args.serverID, partIdx)
      writers(id) = sender()
      registeredWriters += 1
      if (registeredWriters >= args.numServers) {
        //once all the writers have registered, we can start working
        log.info("Reader {}.{} is becoming active", args.serverID, partIdx)
        context.become(activeReader)
        notifyWriters()
      }

    case RequestLog(requestServerId) if requestServerId < args.numServers =>
      // we are not active yet, whatever writer ask for work will have to block for a bit
      blockedWriters(requestServerId) = true
  }

  //active working mode, deal with log requests from writers
  def activeReader: Receive = {

    ////////////////// log distribution logic
    case RequestLog(requestServerId) if requestServerId < args.numServers =>
      // find the queue for this writer partition
      val partitionQueue = logsToDeliver(requestServerId)
      try {
        //get the available log line
        val log = partitionQueue.dequeue()
        //send it over
        sender() ! WriteLog(args.serverID, log, partIdx)
        //refill the queue
        fillUpQueue()
      } catch {
        case NonFatal(e) =>
          // the queue was empty
          if (!lines.hasNext) {
            //we might be at the end of our workload
            checkEnd()
          } else {
            //the queue might have been empty for that partition as we don't have
            // any more logs available
            // mark the writer as blocked to send it a message later
            blockedWriters(requestServerId) = true
            //fill up the queue
            fillUpQueue()
          }
      }
  }

  // a lazy iterator over the lines of the file we are assigned. inputFiles are supposed to be contiguous, sorted
  //  buckets.
  def lineIterator(): BufferedIterator[IdAndLine] = {
    //use an iterator to lazily load the lines
    // make it buffered so we can peak at the head of the iterator
    //new JoinIterators(inputFiles).map(IdAndLine).buffered
    inputFiles.toIterator.flatMap(f=> Source.fromFile(f).getLines.map(IdAndLine)).buffered
  }

  // when we are done filling the queues, notify the writers that might be blocked and for which a line is available
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
        //check the top of the file
        val logLine = lines.head
        //assign a partition to that log line
        logLine.partition(args.numServers) match {
          case Some(partition) =>
            //valid line, find the right queue and push it
            val partitionQueue = logsToDeliver(partition)
            if (partitionQueue.size < args.maxReadBuffer) {
              //but only if the buffer is not full, otherwise, we are done filling up queues
              partitionQueue.enqueue(logLine)
              // move the iterator we just read with
              lines.next()
              // start again until we can't read more messages
              recurseFillUp()
            }
          case None =>
            //invalid line, cannot be parsed, drop it
            log.debug("{} couldn't be parsed", logLine)
            // move the iterator forward
            lines.next()
            // try to fill with the next line
            recurseFillUp()
        }
      }
    }

    //try to fill the buffers
    recurseFillUp()
    //let the writers know that we have more work (maybe)
    notifyWriters()
  }

  // go the writer on each node to register this reader for work pulling
  def register(member: Member): Unit = {
    if (member.hasRole("logsplit")) {
      context.actorSelection(RootActorPath(member.address) / "user" / "writer") ! RegisterReader(args.serverID, partIdx, totalReaders)
    }
  }

  //we are at the end of the read when the lines are done and all
  // queues have drained
  def checkEnd(): Unit = {
    if (!lines.hasNext && logsToDeliver.forall(_.isEmpty)) {
      log.info("Reader {}.{} is done", args.serverID, partIdx)
      // we are done, send a message to all writers
      writers.foreach(_ ! LogDone(args.serverID, partIdx))
      //  and stop this actor
      context.stop(self)
    }
  }
}

