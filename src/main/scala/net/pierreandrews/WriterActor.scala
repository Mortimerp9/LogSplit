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
    val child = context.actorOf(Props(new WriterWorkerActor(args)))
    context.watch(child)
    child
  }

  var doneWorkers: Int = 0

  override def receive: Actor.Receive = LoggingReceive {
    case msg @ RegisterReader(_, readerPart) =>
      //register the reader on one of the worker
      val worker = workers(readerPart % workers.size)
      worker.forward(msg)
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
class WriterWorkerActor(args: LogSplitAppArgs) extends Actor {

  //we do not want to keep file handles open for all possible userIDs
  // so we keep an LRU cache
  val fileCache = new FileCache(args)

  //how many readers are still active
  var readerCnt = 0

  var readers: mutable.ListBuffer[ActorRef] = mutable.ListBuffer()

  override def receive: Receive = LoggingReceive {
    case RegisterReader(readerId, readerPart) =>
      readers.prepend(sender())
      register(sender(), readerId, readerPart)

    case WriteLog(readerId, log, partIdx) =>
      fileCache.write(log, readerId, partIdx)
      readers.foreach(_ ! RequestLog(serverId = args.serverID))

    case LogAvailable =>
      sender() ! RequestLog(serverId = args.serverID)

    case LogDone(_) =>
      println("log done")
      readerCnt -= 1
      if (readerCnt == 0) {
        println("no more readers, closing writer")
        fileCache.close()
        context.stop(self)
      }

  }

  def register(reader: ActorRef, readerId: Int, readerPart: Int): Unit = {
    println(s"reader $readerId.$readerPart registering on ${args.serverID}")
    readerCnt += 1
    reader ! RegisterWriter(args.serverID)
    reader ! RequestLog(serverId = args.serverID)
  }
}

