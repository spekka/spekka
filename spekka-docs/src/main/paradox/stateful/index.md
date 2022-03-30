@@@index
* [Event Based](event-based.md)
* [Durable State](durable-state.md)
* [Usage](usage.md)
* [Akka Persistence Backend](akkapersistence.md)
* [Cluster Sharding](sharding.md)
@@@

# Spekka Stateful

[![javadoc](https://javadoc.io/badge2/io.github.spekka/spekka-stateful_2.13/javadoc.svg)](https://javadoc.io/doc/io.github.spekka/spekka-stateful_2.13/)
[![maven](https://img.shields.io/maven-central/v/io.github.spekka/spekka-stateful_2.13)](https://mvnrepository.com/artifact/io.github.spekka/spekka-stateful_2.13/)

To use this library add the following dependencies:

@@dependency[sbt,Maven,Gradle] {
  symbol="AkkaVersion"  value="2.6.16"
  group="io.github.spekka"  artifact="spekka-stateful_$scala.binary.version$"  version="$project.version$"
  group2="com.typesafe.akka" artifact2="akka-stream_$scala.binary.version$"  version2="AkkaVersion"
  group3="com.typesafe.akka" artifact3="akka-stream-typed_$scala.binary.version$"  version3="AkkaVersion"
  group4="com.typesafe.akka" artifact4="akka-actor-typed_$scala.binary.version$"  version4="AkkaVersion"
}

## Motivation

Working with stateful stream is at the same time a quite common and complex task.

While Akka Streams does offer all the required building blocks to work with stateful flows, the overall experience is hampered by the low level nature of the library: it's often necessary to drop down to the `GraphStage` API level to implement common tasks and in general the user its left without guidance in the design of the solution. Furthermore there is no stream native encoding of persistent stateful flows, requiring users to create their own custom solution.

The goal of Spekka Stateful is to provide an opinionated, stream native way to define stateful flow with the following properties:

- clean distinction between business-logic and state-management
- structured way to interact with stateful flows from outside the stream
- state persistence support

## Overview

The idea of the library is that a stateful flow is defined in terms of:

- The business logic: `StatefulFlowLogic`
- The way to manage the state: `StatefulFlowBackend`

A `StatefulFlowLogic[State, In, Out, Command]` is responsible of defining how the state is altered in response to a stream element / command: it encodes the business logic of the stream. The logic doesn't need to concern itself on the details of how the state is managed as this will be handled by an instance of `StatefulFlowBackend`.

Combining a logic and a backend instance we obtain a `StatefulFlowProps` which is a complete description of how the flow will work. Since the same stateful flow could be used for multiple purposes we need a way to tie the flow description to a specific semantic instance.

To achieve this we register the flow description on a `StatefulFlowRegistry` specifying the `entityKind` of the flow (i.e. which kind of entities will be computed). The `StatefulFlowBuilder` which we get as a result, encodes both how the flow works (what it computes and how it manages the state) as well as what the flow purpose is.

The `StatefulFlowBuilder` can now be used to obtain flow for particular `entityId` via the method `StatefulFlowBuilder.flow`. The result is a standard Akka `Flow` with a materialized value containing a `StatefulFlowControl` object which can be used to interact with the flow from outside the stream by sending *commands*.

The library supports 2 kinds of stateful flows:

1. **Event based:** stream elements and commands do not affect the state directly, but they cause the generation of events which are then used to modify the state. This approach is well suited to implement event sourced systems.
2. **Durable state:** stream elements and commands do affect the state directly: a new state is returned (and if required persisted) after each message/command handling.



