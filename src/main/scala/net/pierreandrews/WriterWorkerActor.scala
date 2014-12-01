package net.pierreandrews

import scala.collection.mutable

import akka.actor._
import net.pierreandrews.Protocol._
import net.pierreandrews.utils.FileCache

/**
 * A write workers is responsible for pulling data from a set of readers and push them on the local filesystem.
 *
 * The WriterActor manager assigns readers to this worker transparently. The worker doesn't know that it's part of a
 * cluster and if the logs are coming from a remote actor or not.
 *
 * This worker get work by pulling it from the reader. This allows for a better flow control and avoids overloading the
 * akka and cluster messages queues when the writer blocks.
 *
 * The worker starts in a mode that only accepts registering messages, when all the cluster is up and readers have all
 * registered, we switch (become) to another mode where we query the reader for work and write logs to the local filesystem
 * (using FileCache).
 *
 * When the worker pulls work from the reader, this one might not have any lines currently available for this partition,
 * in this case, the writer becomes idle and will only start pulling work again when a reader sends it a LogAvailable message.
 *
 * While a writer actor can deal with multiple readers from multiple nodes, we know that:
 *  - each reader is only assigned one single writer from each node
 *  - each reader assigned to a worker will not be assigned to another reader
 * This means that no one else will try to write concurrently to the local files assigned to this worker.
 *
 * User: pierre
 * Date: 11/28/14
 */
class WriterWorkerActor(args: LogSplitAppArgs, writerId: Int) extends Actor with ActorLogging {

  //we do not want to keep file handles open for all possible userIDs
  // so we keep an LRU cache of opened file handles.
  val fileCache = new FileCache(args)

  //how many readers are still active. When this becomes 0 again, we are done pulling work.
  var readerCnt = 0

  //keep track of the registered readers
  val readers: mutable.ListBuffer[ActorRef] = mutable.ListBuffer()
  // keep track of readers that are replying (avoid flooding readers that are not responding)
  val activeReaders: mutable.Map[(Int, Int), ActorRef] = mutable.Map()

  override def postStop(): Unit = {
    fileCache.close()
  }

  override def receive: Receive = {

    // All the cluster is up and readers have all been assigned to writers,
    // the manager is telling us that we can start working
    case StartReading =>
      if (readerCnt == 0) {
        //no readers assigned to this writer, just stop it straight away
        context.stop(self)
      } else {
        // otherwise, we should become active
        log.info("Writer {}.{} is becoming active", args.serverID, writerId)
        context.become(activeWriter)
        //  and start pulling work from the reader
        requestLogs()
      }

    case RegisterReader(readerServer, readerPart, _) =>
      // a reader is being assigned to us by the manager. Because we used forward, sender() refers to the
      // reader worker and not the manager.
      register(sender(), readerServer, readerPart)

  }

  // request more work from the readers
  def requestLogs(): Unit = {
    activeReaders.foreach {
      case (k, v) =>
        //once we have requested work from a reader
        // we shouldn't request more work until it answers
        activeReaders.remove(k)
        v ! RequestLog(args.serverID)
    }
  }

  def activeWriter: Receive = {
    // the worker is active, it can either:
    // - receive a log from a reader to write down locally
    case WriteLog(readerServer, log, partIdx) =>
      fileCache.write(log, readerServer, partIdx)
      // the reader replied, add it back to the list of active readers
      activeReaders.put((readerServer, partIdx), sender())
      requestLogs()

    // - receive a notification from a reader that work is available for this writer
    case LogAvailable =>
      sender() ! RequestLog(serverId = args.serverID)

    // - receive a notification that a particular reader already loaded all the logs
    // files that were assigned to him and that we shouldn't expect anything from it.
    // Because this writer pulls work from multiple readers, it might receive mode than
    // one of this message and should make sure to shutdown only when all the readers are done.
    case LogDone(readerServer, partIdx) =>
      //remove the actor reference so we stop asking it for work
      activeReaders -= ((readerServer, partIdx))
      log.info("log done for {}.{}", readerServer, partIdx)
      readerCnt -= 1
      //do not stop the writer until ALL partitions are done
      if (readerCnt == 0) {
        log.info("no more readers, closing writer {}.{}", args.serverID, writerId)
        context.stop(self)
      }

  }

  // register an actor on this writer
  def register(reader: ActorRef, readerServer: Int, readerPart: Int): Unit = {
    log.info("Reader {}.{} registering on {}.{}", readerServer, readerPart, args.serverID, writerId)
    activeReaders.put((readerServer, readerPart), sender())
    readers.prepend(sender())
    readerCnt += 1
    //register the writer on the reader.
    reader ! RegisterWriter(args.serverID)
  }
}
