package net.pierreandrews

import java.io.{ FilenameFilter, File }

import akka.actor.{ Props, ActorSystem }
import com.quantifind.sumac.validation.{ Positive, Required }
import com.quantifind.sumac.{ FieldArgs, ArgMain }
import com.typesafe.config.ConfigFactory
import net.pierreandrews.utils.LogSplitUtils

/**
 * The main application that runs on one server. This will initialize the actor system on
 *  the current server and setup the cluster properties.
 *
 * See `LogSplitApp --help` and LogSplitAppArgs to see the valid and required command line arguments.
 *  The default is to run on a localhost cluster, where we have a node at localhost:2550, localhost:2551 and
 *  localhost:2552. You should start at least three JVMs one these ports (using --port 2550, etc.). You
 *  can run the node on separate machines, then you have to specify --seeds to point to the other machines, or update
 *  application.conf
 *
 *
 * User: pierre
 * Date: 11/28/14
 */
object LogSplitApp extends ArgMain[LogSplitAppArgs] {
  override def main(args: LogSplitAppArgs): Unit = {

    //setup the actorsystem and cluster connection
    val config = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=${args.port}")
    //if we have a set of seeds on the command line
    val configWithSeeds = args.seeds.map { seedSeq =>
      val seedStr = seedSeq.map(ip => s"""akka.tcp://ClusterSystem@$ip""").mkString(",")
      config.withFallback(ConfigFactory.parseString(s"akka.cluster.seed-nodes=[$seedStr]"))
    }.getOrElse(config)
      //and finally load application.conf and co
      .withFallback(ConfigFactory.load())

    //start the system
    val system = ActorSystem(s"ClusterSystem", configWithSeeds)

    //setup some file readers
    startReaders(args, system)

    //setup the sorter manager for this node
    val sorter = system.actorOf(Props(new SorterActor(args)), name = "sorter")

    //setup the writer manager for this node
    system.actorOf(Props(new WriterActor(args, sorter)), name = "writer")

  }

  //we assume that the log files are split in buckets and follow this naming pattern (see below)
  private final val validLogFile = ".[0-9]+.log$".r

  //setup the reader workers
  def startReaders(args: LogSplitAppArgs, system: ActorSystem): Unit = {
    //listFiles might return null, wrap in Option
    val files: Seq[File] = Option(args.input.listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = return validLogFile.findFirstIn(name).isDefined
    })).map(_.toSeq)
      .getOrElse(Seq())
      .sortBy { file =>
        //we assume that log files will have the form: filename.NUMBER.log
        // and that filename.0.log has more recent logs than filename.1.log, etc.
        // (this is a bit naive maybe)
        val name = file.getName
        val dotIdx = name.indexOf('.')
        val secondDot = name.indexOf('.', dotIdx + 1)
        val partID = name.substring(dotIdx + 1, secondDot)
        partID.toInt
      }

    // distribute the input files between each workers
    // zipWithIndex assignes them an id
    LogSplitUtils.cut(files, args.numReaderWorkers).zipWithIndex.foreach {
      case (files, partIdx) =>
        // for each input split, start a reader that will read the files in
        // parallel
        system.actorOf(Props(new ReaderActor(args, files, partIdx)), name = s"reader-$partIdx")
    }
  }
}

/**
 * Command line argument definition, using Sumac.
 *
 * This settings are local to a node/server.
 *
 */
class LogSplitAppArgs extends FieldArgs {

  // The id of the server, this is required
  // the ID has to be positive and is used to assign users to
  // this server (partition)
  @Required
  var serverID: Int = -1

  // What port are we running in.
  // It is important that this corresponds to the settings in application.conf
  //  or provided by --seeds
  var port: Int = 0
  // Where are we reading the logs from
  @Required
  var input: File = _
  // Where the files going to be output
  // part files go here too
  @Required
  var output: File = _

  //How many servers are there.
  // This is set to default to 3.
  // It is important that this is accurate as it is used to assign users to partitions.
  // Each node in the cluster should be configured with the same numServers
  @Positive
  var numServers: Int = 3

  // How many writer workers do we want on this server
  @Positive
  var numWriteWorkers: Int = 5
  // how many file handles should EACH writer worker keep cached. See the WriterWorkerActor for more details
  @Positive
  var maxWriteOpen: Int = 5


  //How many reader workers do we want on this server?
  @Positive
  var numReaderWorkers: Int = 10
  // We buffer log lines in memory,
  // this is useful to unblock reads when a particular partition is slower than another one
  // a higher value should increase the throughput but will require more memory.
  // Each reader will load that amount of lines for each partition in memory, so the actual lines
  // in memory will be potentially = numReaderWorkers*numServers*maxReadBuffer
  @Positive
  var maxReadBuffer: Int = 1000

  //Once the log distribution is done, we are left with a number of part files coming from each server
  // each part file is sorted but all the parts are not globally sorted. Once we are done collecting logs,
  // we have to sort the lines again. How many parallel sorters should we use.
  @Positive
  var numSortWorkers: Int = 5

  //Should we delete the partially sorted part files when we are done.
  var deletePartFiles: Boolean = true

  //settings for the cluster seeds. The defaults are set in application.conf. This has to be specified as a list of string IP with port:
  // e.g. --seeds 192.168.0.1:2550,192.168.0.2:2551,192.168.0.30:2552
  // at least one seed should be setup, the other nodes should be able to auto-discover each other from this.
  var seeds: Option[Seq[String]] = None

  addValidation {
    require(port >= 0, "port cannot be negative")
    require(serverID >= 0, "serverID cannot be negative")
  }

}