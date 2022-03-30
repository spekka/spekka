# Spekka Stateful Akka Persistence

[![javadoc](https://javadoc.io/badge2/io.github.spekka/spekka-stateful-akkapersistence_2.13/javadoc.svg)](https://javadoc.io/doc/io.github.spekka/spekka-stateful-akkapersistence_2.13/)
[![maven](https://img.shields.io/maven-central/v/io.github.spekka/spekka-stateful-akkapersistence_2.13)](https://mvnrepository.com/artifact/io.github.spekka/spekka-stateful-akkapersistence_2.13/)

This library provides `StatefulFlowBackend.EventBased` and `StatefulFlowBackend.DurableState` implementations based on Akka Persistence.

To use this library add the following dependencies:

@@dependency[sbt,Maven,Gradle] {
  symbol="AkkaVersion"  value="2.6.16"
  group="io.github.spekka"  artifact="spekka-stateful_$scala.binary.version$"  version="$project.version$"
  group2="com.typesafe.akka" artifact2="akka-stream_$scala.binary.version$"  version2="AkkaVersion"
  group3="com.typesafe.akka" artifact3="akka-stream-typed_$scala.binary.version$"  version3="AkkaVersion"
  group4="com.typesafe.akka" artifact4="akka-actor-typed_$scala.binary.version$"  version4="AkkaVersion"
  group5="com.typesafe.akka" artifact5="akka-persistence-typed_$scala.binary.version$"  version5="AkkaVersion"
}

@@@warn
You will also need to specify the dependency for the actual Akka Persistence plugin you are using.
@@@

## Event Based

In order to create an event based Akka Persistence backend you can  use: `AkkaPersistenceStatefulFlowBackend.EventBased[State, Ev]`.

You can specify:

- The persistence plugin (see Akka Persistence)
- The retention criteria, disabled by default (see Akka Persistence)
- The number of partitions to tag the events, 1 by default (see Akka Projection)
- The parallelism used to run side effects, 1 by default

You can further modify the backends by using the following methods:

- `withEventAdapter`: Specify an event adapter (see Akka Persistence)
- `withSnapshotAdapter`: Specify a snapshot adapter (see AKka Persistence)
- `withEventCodec`: Specify an implicit `Codec[Ev]` instance making event serialization handled transparently by Spekka
- `withSnapshotCodec`: Specify an implicit `Codec[State]` instance making state serialization handled transparently by Spekka

## Durable State

In order to create a durable state Akka Persistence backend you can use: `AkkaPersistenceStatefulFlowBackend.DurableState[State]`.

You can specify:

- The persistence plugin (see Akka Persistence)
- The parallelism used to run side effects, 1 by default

You can further modify the backends by using the following methods:

- `withSnapshotAdapter`: Specify a snapshot adapter (see AKka Persistence)
- `withSnapshotCodec`: Specify an implicit `Codec[State]` instance making state serialization handled transparently by Spekka