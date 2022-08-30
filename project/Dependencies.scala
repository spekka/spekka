import sbt._

object Dependencies {
  lazy val AkkaVersion = "2.6.16"

  lazy val `akka-actor-typed` = "com.typesafe.akka" %% "akka-actor-typed" % AkkaVersion

  lazy val `akka-stream` = "com.typesafe.akka" %% "akka-stream" % AkkaVersion
  lazy val `akka-stream-typed` = "com.typesafe.akka" %% "akka-stream-typed" % AkkaVersion

  lazy val `akka-persistence-typed` = "com.typesafe.akka" %% "akka-persistence-typed" % AkkaVersion

  lazy val `akka-cluster-sharding` =
    "com.typesafe.akka" %% "akka-cluster-sharding-typed" % AkkaVersion

  lazy val `akka-stream-testkit` = "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion
  lazy val `akka-persistence-testkit` =
    "com.typesafe.akka" %% "akka-persistence-testkit" % AkkaVersion

  lazy val `akka-slf4j` = "com.typesafe.akka" %% "akka-slf4j" % AkkaVersion

  lazy val scalaTest = "org.scalatest" %% "scalatest" % "3.2.13"

  lazy val `logback` = "ch.qos.logback" % "logback-classic" % "1.4.0"
}
