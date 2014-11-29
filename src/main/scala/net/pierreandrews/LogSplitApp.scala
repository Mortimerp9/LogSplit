package net.pierreandrews

import java.io.File

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
    val reader = system.actorOf(Props(new ReaderActor(args)), name = "reader")
    val sorter = system.actorOf(Props(new SorterActor(args)), name = "sorter")
    //TODO one writer per part? use inner actor to writer?
    val writer = system.actorOf(Props(new WriterActor(args, sorter)), name = "writer")

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
  var maxWriteOpen: Int = 10

  var numServers: Int = 3
}