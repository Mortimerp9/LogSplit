package net.pierreandrews

import akka.actor.Actor
import akka.event.LoggingReceive
import net.pierreandrews.Protocol.StartSorting

/**
 * Manages the sorting of the partial files for each userid
 * User: pierre
 * Date: 11/28/14
 */
class SorterActor(args: LogSplitAppArgs) extends Actor {
  override def receive: Receive = LoggingReceive {
    case StartSorting =>  //TODO
    println("START SORTING")
  }
}
