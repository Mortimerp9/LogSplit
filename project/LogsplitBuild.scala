import sbt._
import sbt.Keys._

object LogsplitBuild extends Build {

  lazy val logsplit = Project(
    id = "logsplit",
    base = file("."),
    settings = Project.defaultSettings ++ Seq(
      name := "LogSplit",
      organization := "net.pierreandrews",
      version := "0.1-SNAPSHOT",
      scalaVersion := "2.11.4",
      resolvers += "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases",
      libraryDependencies ++= Seq(
        "com.typesafe.akka" %% "akka-cluster" % "2.3.7",
        "com.typesafe.akka" %% "akka-actor" % "2.3.7",
        "com.quantifind" %% "sumac" % "0.3.0",
        "org.scalatest" %% "scalatest" % "2.2.1" % "test",
        "org.apache.commons" % "commons-collections4" % "4.0",
        "joda-time" % "joda-time" % "2.5",
        "org.joda" % "joda-convert" % "1.2")))
}
