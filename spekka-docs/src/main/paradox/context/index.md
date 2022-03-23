@@@index

* [Flow With Extended Context](flow-with-extended-context.md)
* [Partitions](partitions.md)
@@@

# Spekka Context

[![javadoc](https://javadoc.io/badge2/io.github.spekka/spekka-context_2.13/javadoc.svg)](https://javadoc.io/doc/io.github.spekka/spekka-context_2.13/)
[![maven](https://img.shields.io/maven-central/v/io.github.spekka/spekka-context_2.13)](https://mvnrepository.com/artifact/io.github.spekka/spekka-context_2.13/)

To use this library add the following dependencies:

@@dependency[sbt,Maven,Gradle] {
  symbol="AkkaVersion"  value="2.6.16"
  group="io.github.spekka"  artifact="spekka-context_$scala.binary.version$"  version="$project.version$"
  group2="com.typesafe.akka" artifact2="akka-stream_$scala.binary.version$"  version="$project.version$" version2="AkkaVersion"
  group3="com.typesafe.akka" artifact3="akka-stream-typed_$scala.binary.version$"  version="$project.version$" version3="AkkaVersion"
}

## Motivation

The objective of the library is to simplify the modeling and usage of *one-to-one* flows. 
A *one-to-one* flow is a flow that given an input, will produce exactly one output.

In order to understand why such a kind of flow would be desirable, let's think of a simple example. 
There is some process producing data in a Kafka topic.
We need to read this data and for each message invoke some remote API guaranteeing an *at-least-once* semantic.

This is pretty straightforward to do using Akka Streams and Alpakka Kafka:

```scala
val kafkaReaderSource: Source[(Data, Consumer.CommittableOffset), NotUsed] = ???

val remoteApiCallFlow: Flow[(Data, Consumer.CommittableOffset), Consumer.CommittableOffset, NotUsed] = ???

val kafkaCommitterSink: Sink[Consumer.CommittableOffset, NotUsed] = ???

kafkaReaderSource.via(remoteApiCallFlow).runWith(kafkaCommitterSink)

```

At a first glance this approach might seem fine:

- we are using Kafka offset to keep track of our position in the topic
- we are committing offset only after the API call have been performed

However if we dig a little bit deeper we can see that there are 2 major problems:

- we have no guarantee that `remoteApiCallFlow` maintains the order of the messages
- receiving an offset as output of `remoteApiCallFlow` does not gives us any guarantee that we will not receive the same value in the future (i.e. there are `mapConcat` operations inside the flow).

These 2 observations are enough to conclude that we cannot guarantee an *at-least-once* semantics: in both cases we might end up
skipping ahead and committing an offset higher than the one for which we have successfully performed the work; we are now a restart away from loosing some messages without even realizing it.

## Overview

This library provides the following components:

- Extended contexts
- Flows with extended contexts
- Partitioning support for flows with extended contexts

An `ExtendedContext[Ctx]` is a simple wrapper on a generic context type `Ctx` which allows the library to add information to
stream elements in a transparent way for the user. These additional information are used to allows features like automatic back-pressured outputs reordering, context multiplexing and more.

A `FlowWithExtendedContext[In, Out, Ctx, M]` is much like an Akka's `FlowWithContext` where the context type `Ctx` is wrapped in an `ExtendedContext`. The allowed operations on a `FlowWithExtendedContext` are limited in order to avoid:

- filtering operation (like `filter`, `filterNot`, `collect`)
- *one-to-n* (like `mapConcat`)

@@@ warning
It comes a point were such restrictions may make it impossible to implement a particular feature.
For this reason there is the possibility to convert a `FlowWithExtendedContext` to a regular flow (using `toGraph`) and back 
(using `FlowWithExtendedContext.fromGraphUnsafe`).

This method has an intentionally *scary* name as, when using it, the programmer is responsible of making sure that the
`FlowWithExtendedContext` guarantees have been respected. Failure to do so may result in stream deadlock and/or errors at run-time.
@@@

The last feature offered by the library is the partitioning support for `FlowWithExtendedContext`. The goal is to enable
a boilerplate free, type safe and dynamic definition of partition trees with support for both unicast and multicast routing strategies.