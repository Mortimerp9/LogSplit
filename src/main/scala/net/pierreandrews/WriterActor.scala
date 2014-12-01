package net.pierreandrews

import akka.actor._
import akka.event.LoggingReceive
import net.pierreandrews.Protocol._

/**
 * The main writer work manager. There is one WriterActor per server and it creates a number
 *  of workers. Each worker is responsible for a number of files, as decided by the reader's part id.
 *
 *  Each worker runs in parallel and is responsible for files that no one else will write to.
 *
 *  The writers are not aware of the cluster, they just receive registration from the readers and don't really care where
 *  they live. The readers could be reading files locally or on another server.
 *
 * User: pierre
 * Date: 11/29/14
 */
class WriterActor(args: LogSplitAppArgs, sorter: ActorRef) extends Actor {

  /*
   * start a set of worker and watch them, when they get terminated, we know that we are 
   * done with all writes
   */
  val workers: IndexedSeq[ActorRef] = for { i <- 0 until args.numWriteWorkers } yield {
    val child = context.actorOf(Props(new WriterWorkerActor(args, i)))
    context.watch(child)
    child
  }

  //count the number of workers that are done (all their readers have emptied their files)
  var doneWorkers: Int = 0

  /*
   * For each server, keep track of the number of readers that will register with us. This
    * allows the writer to know when the cluster is all up and it can start pulling work
    * from the readers.
    *
    * Each cell corresponds to one server.
   */
  val awaitReaders: Array[Int] = {
    val seq = new Array[Int](args.numServers)
    for (i <- 0 until args.numServers) {
      //set the value to -1, which means we don't know yet, this
      // will be populated properly on the first message from a reader for this server
      seq(i) = -1
    }
    seq
  }

  override def receive: Actor.Receive = LoggingReceive {
    case msg @ RegisterReader(readerServer, readerPart, totalReader) =>
      //register the reader on one of the worker
      val worker = workers(readerPart % workers.size)
      worker.forward(msg)

      //populate the awaitReaders array for that server
      if (awaitReaders(readerServer) == -1) awaitReaders(readerServer) = totalReader
      awaitReaders(readerServer) -= 1

      //when all the servers are done initializing (we got a register message from all workers), we can
      // start pulling from them
      if (awaitReaders.forall(_ == 0)) {
        //let the workers know
        workers.foreach(_ ! StartReading)
      }

    case Terminated(_) =>
      // one of the worker finished
      doneWorkers += 1
      if (doneWorkers >= workers.size) {
        //all workers are done pulling logs from the readers. We can now start sorting the part collected for each user
        sorter ! StartSorting
        //this actor is now useless, just get rid of it
        context.stop(self)
      }
  }
}

