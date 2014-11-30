package net.pierreandrews

import java.io.{ FilenameFilter, File }

import akka.actor.{ Props, ActorSystem }
import com.quantifind.sumac.validation.Required
import com.quantifind.sumac.{ FieldArgs, ArgMain }
import com.typesafe.config.ConfigFactory
import net.pierreandrews.utils.LogSplitUtils

/**
 * TODO DOC
 * User: pierre
 * Date: 11/28/14
 */
object LogSplitApp extends ArgMain[LogSplitAppArgs] {
  override def main(args: LogSplitAppArgs): Unit = {
    val config = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=${args.port}")
      .withFallback(ConfigFactory.parseString("akka.cluster.roles = [logsplit]"))

    val configWithSeeds = args.seeds.map { seedSeq =>
      val seedStr = seedSeq.map('"' + _ + '"').mkString(",")
      config.withFallback(ConfigFactory.parseString(s"akka.cluster.seed-nodes=[$seedStr]"))
    }.getOrElse(config)
      .withFallback(ConfigFactory.load())

    val system = ActorSystem(s"ClusterSystem", configWithSeeds)
    startReaders(args, system)
    val sorter = system.actorOf(Props(new SorterActor(args)), name = "sorter")
    system.actorOf(Props(new WriterActor(args, sorter)), name = "writer")

  }

  private final val validLogFile = ".[0-9]+.log$".r


  def startReaders(args: LogSplitAppArgs, system: ActorSystem): Unit = {
    //listFiles might return null, wrap in Option
    val files: Seq[File] = Option(args.input.listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = return validLogFile.findFirstIn(name).isDefined
    })).map(_.toSeq)
      .getOrElse(Seq())
      .sortBy { file =>
      //we assume that log files will have the form: filename.NUMBER.log
      // and that filename.0.log has more recent logs than filename.1.log, etc.
      val name = file.getName
      val dotIdx = name.indexOf('.')
      val secondDot = name.indexOf('.', dotIdx+1)
      val partID = name.substring(dotIdx+1,secondDot)
      partID.toInt
    }

    //for each input split, start a reader that will read the files in
    // parallel
    LogSplitUtils.cut(files, args.numReaderWorkers).zipWithIndex.foreach {
      case (files, partIdx) =>
        system.actorOf(Props(new ReaderActor(args, files, partIdx)), name = s"reader-$partIdx")
    }
  }
}

class LogSplitAppArgs extends FieldArgs {

  @Required
  var serverID: Int = -1
  var port: Int = 0
  @Required
  var input: File = _
  @Required
  var output: File = _

  var maxReadBuffer: Int = 1000
  var maxWriteOpen: Int = 5
  var numWriteWorkers: Int = 5
  var numReaderWorkers: Int = 5
  var numSortWorkers: Int = 5

  var numServers: Int = 3

  var seeds: Option[Seq[String]] = None
}