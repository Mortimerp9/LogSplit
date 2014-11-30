package net.pierreandrews

import java.io.{FilenameFilter, File}

import akka.actor.{Props, ActorSystem}
import com.quantifind.sumac.validation.Required
import com.quantifind.sumac.{FieldArgs, ArgMain}
import com.typesafe.config.ConfigFactory

/**
 * TODO DOC
 * User: pierre
 * Date: 11/28/14
 */
object LogSplitApp extends ArgMain[LogSplitAppArgs] {
  override def main(args: LogSplitAppArgs): Unit = {
    val config = ConfigFactory.parseString(s"akka.remote.netty.tcp.port=${args.port}").
      withFallback(ConfigFactory.parseString("akka.cluster.roles = [logsplit]")).
      withFallback(ConfigFactory.load())

    val system = ActorSystem(s"ClusterSystem", config)
    //TODO create one reader per part? user inner actor?
    startReaders(args, system)
    val sorter = system.actorOf(Props(new SorterActor(args)), name = "sorter")
    //TODO one writer per part? use inner actor to writer?
    system.actorOf(Props(new WriterActor(args, sorter)), name = "writer")

  }

  def startReaders(args: LogSplitAppArgs, system: ActorSystem): Unit = {
    //listFiles might return null, wrap in Option
    val files: Seq[File] = Option(args.input.listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = return name.endsWith(".log")
    })).map(_.toSeq).getOrElse(Seq())

    //for each input split, start a reader that will read the files in
    // parallel
    files.zipWithIndex.foreach {
      case (file, partIdx) =>
        system.actorOf(Props(new ReaderActor(args, file, partIdx)), name = s"reader-$partIdx")

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
  var numWriteWorkers: Int = 10

  var numServers: Int = 3
}