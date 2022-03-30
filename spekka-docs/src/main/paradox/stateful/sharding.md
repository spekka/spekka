# Spekka Stateful Sharding

[![javadoc](https://javadoc.io/badge2/io.github.spekka/spekka-stateful-sharding_2.13/javadoc.svg)](https://javadoc.io/doc/io.github.spekka/spekka-stateful-sharding_2.13/)
[![maven](https://img.shields.io/maven-central/v/io.github.spekka/spekka-stateful-sharding_2.13)](https://mvnrepository.com/artifact/io.github.spekka/spekka-stateful-sharding_2.13/)

This library extends Spekka's stateful flows by distributing them on a cluster via Akka Cluster Sharding.

To use this library add the following dependencies:

@@dependency[sbt,Maven,Gradle] {
  symbol="AkkaVersion"  value="2.6.16"
  group="io.github.spekka"  artifact="spekka-stateful_$scala.binary.version$"  version="$project.version$"
  group2="com.typesafe.akka" artifact2="akka-stream_$scala.binary.version$" version2="AkkaVersion"
  group3="com.typesafe.akka" artifact3="akka-stream-typed_$scala.binary.version$"   version3="AkkaVersion"
  group4="com.typesafe.akka" artifact4="akka-actor-typed_$scala.binary.version$"  version4="AkkaVersion"
  group5="com.typesafe.akka" artifact5="akka-cluster-sharding-typed_$scala.binary.version$"   version5="AkkaVersion"
}

## Usage

In order to use the sharding support you need to create a `ShardedStatefulFlowRegistry`:

@@snip[Registry.scala](/spekka-stateful-sharding/src/test/scala/spekka/stetful/ShardedStatefulFlowSuite.scala) { #registry }

and use the obtained *sharded registry* to register those flows you want to be sharded on the cluster.

The `StatefulFlowBuilder` obtained as a result of the registration can be used as if it were registered on the base registry.

