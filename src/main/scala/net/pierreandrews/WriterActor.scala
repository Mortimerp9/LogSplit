package net.pierreandrews

import scala.collection.mutable

import akka.actor._
import akka.cluster.Cluster
import akka.event.LoggingReceive
import net.pierreandrews.Protocol._
import net.pierreandrews.utils.FileCache

/**
 * TODO DOC
 * User: pierre
 * Date: 11/29/14
 */
class WriterActor(args: LogSplitAppArgs, sorter: ActorRef) extends Actor {

  val cluster = Cluster(context.system)

  val workers: IndexedSeq[ActorRef] = for { i <- 0 until args.numWriteWorkers } yield {
    val child = context.actorOf(Props(new WriterWorkerActor(args, i)))
    context.watch(child)
    child
  }

  var doneWorkers: Int = 0

  val awaitReaders: Array[Int] = {
    val seq = new Array[Int](args.numServers)
    for (i <- 0 until args.numServers) {
      seq(i) = -1
    }
    seq
  }

  override def receive: Actor.Receive = LoggingReceive {
    case msg @ RegisterReader(readerServer, readerPart, totalReader) =>
      //register the reader on one of the worker
      val worker = workers(readerPart % workers.size)
      worker.forward(msg)
      if (awaitReaders(readerServer) == -1) awaitReaders(readerServer) = totalReader
      awaitReaders(readerServer) -= 1
      if (awaitReaders.forall(_ == 0)) {
        workers.foreach(_ ! StartReading)
      }
    case Terminated(_) =>
      doneWorkers += 1
      if (doneWorkers >= workers.size) {
        //all done, start sorting please
        sorter ! StartSorting
        context.stop(self)
      }
  }
}

/**
 * TODO DOC
 * User: pierre
 * Date: 11/28/14
 */
class WriterWorkerActor(args: LogSplitAppArgs, writerId: Int) extends Actor {

  //we do not want to keep file handles open for all possible userIDs
  // so we keep an LRU cache
  val fileCache = new FileCache(args)

  //how many readers are still active
  var readerCnt = 0

  var readers: mutable.ListBuffer[ActorRef] = mutable.ListBuffer()

  override def receive: Receive = LoggingReceive {

    case StartReading =>
      if(readerCnt == 0) {
        //no readers assigned to this writer, just stop it straight away
        context.stop(self)
      } else {
        println(s"Writer ${args.serverID}.$writerId is becoming active")
        context.become(activeWriter)
        readers.foreach(_ ! RequestLog(args.serverID))
      }

    case RegisterReader(readerServer, readerPart, _) =>
      register(sender(), readerServer, readerPart)

  }

  def activeWriter: Receive = LoggingReceive {
    case WriteLog(readerServer, log, partIdx) =>
      fileCache.write(log, readerServer, partIdx)
      readers.foreach(_ ! RequestLog(serverId = args.serverID))

    case LogAvailable =>
      sender() ! RequestLog(serverId = args.serverID)

    case LogDone(readerServer, partIdx) =>
      readers -= sender()
      println(s"log done for $readerServer.$partIdx")
      readerCnt -= 1
      //do not stop the writer until ALL partitions are done
      if (readerCnt == 0) {
        println(s"no more readers, closing writer ${args.serverID}.$writerId")
        fileCache.close()
        context.stop(self)
      }

  }

  def register(reader: ActorRef, readerServer: Int, readerPart: Int): Unit = {
    println(s"reader $readerServer.$readerPart registering on ${args.serverID}.$writerId")
    readers.prepend(sender())
    readerCnt += 1
    reader ! RegisterWriter(args.serverID)
  }
}

