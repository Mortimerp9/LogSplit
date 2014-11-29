package net.pierreandrews

import akka.actor.{Actor, ActorRef, RootActorPath}
import akka.cluster.{Cluster, Member, MemberStatus}
import akka.cluster.ClusterEvent.{CurrentClusterState, MemberUp}
import net.pierreandrews.Protocol._
import net.pierreandrews.utils.FileCache

/**
 * TODO DOC
 * User: pierre
 * Date: 11/28/14
 */
class WriterActor(args: LogSplitAppArgs, sorter: ActorRef) extends Actor {

  val cluster = Cluster(context.system)

  //we do not want to keep file handles open for all possible userIDs
  // so we keep an LRU cache
  val fileCache = new FileCache(args)

  //how many readers are still active
  var readerCnt = 0

  // subscribe to cluster changes, MemberUp
  override def preStart(): Unit = cluster.subscribe(self, classOf[MemberUp])
  // re-subscribe when restart
  override def postStop(): Unit = cluster.unsubscribe(self)

  override def receive: Receive = {
    case state: CurrentClusterState =>
      state.members.filter(_.status == MemberStatus.Up) foreach register
    case MemberUp(m) => register(m)
    case RegisterReader(id) =>
      println(s"reader $id registering on ${args.serverID}")

    case WriteLog(readerId, log) =>
      fileCache.write(log, readerId)
      sender() ! RequestLog(args.serverID)

    case LogDone(_) =>
      readerCnt -= 1
      if (readerCnt == 0) {
        fileCache.close()
        sorter ! StartSorting
        context.stop(self)
      }

  }

  def register(member: Member): Unit = {
    if (member.hasRole("logsplit")) {
      val reader = context.actorSelection(RootActorPath(member.address) / "user" / "reader")
      readerCnt += 1
      reader ! RegisterWriter(args.serverID)
      reader ! RequestLog(args.serverID)
    }
  }
}

