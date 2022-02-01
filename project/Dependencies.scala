import sbt._

object Dependencies {
  lazy val AkkaVersion = "2.6.16"

  lazy val `akka-actor` = "com.typesafe.akka" %% "akka-actor" % AkkaVersion
  lazy val `akka-stream` = "com.typesafe.akka" %% "akka-stream" % AkkaVersion
  lazy val `akka-stream-typed` = "com.typesafe" %% "akka-stream-typed" % AkkaVersion
  lazy val `akka-stream-testkit` = "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion

  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.2.9"
}