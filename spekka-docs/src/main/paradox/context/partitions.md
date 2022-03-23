# Partitions

`FlowWithExtendedContext` are a good abstraction to model *one-to-one* streams, however by themselves they are not enough.

In the @ref:[Flow With Extended Context](./flow-with-extended-context.md) section we showed how to create a stream which computes the total number of people entered in a store. The problem as presented was so simple that a `FlowWithExtendedContext` was just not actually needed, a standard `FlowWithContext` would have be more than enough.

The benefit introduced by `FlowWithExtendedContext` becomes more apparent as soon as we start making the stream a little bit more complicated.

For starters we would like to support more than a single deployment. We could embed the logic of handling multiple deployments in the flow itself and while this would technically work, it forces us to make our logic more and more complicated every time we change how we group things.

An alternative approach would be to use Akka's `SubStream` to partition the stream by deployment and then use the same logic. This approach is definitely more in line with what we would like to achieve however it comes with some disadvantages:
- we cannot access the materialization value of subflows
- we have limited control in how the partitions are managed
- by design Akka sub stream are subject to race-conditions around sub-flow completion and re-materialization (see [Akka's groupBy documentation](https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/groupBy.html)) which may cause stream elements to be lost.

As partitioning data is such a common task, Spekka provides an extensive support for partition via the `Partition` object.

In particular, the standard way to use Spekka's partition support should be to always start
with the following code:

```scala
import PartitionTree._
Partition.treeBuilder[Input, Context]
```

## Dynamic partitioning with automatic materialization

The equivalent of Akka's `groupBy` is represented by Spekka dynamic partition with automatic materialization. This kind of partition works on a dynamic set of keys for which flows are materialized automatically at the first key occurrence.

Let's use this strategy to implement the support for multiple deployment in our people counter stream.

In order to showcase one of the feature of Spekka's partitioning system let's add a communication channel for the stream via the materialization value:

@@snip[FlowDefinition.scala](/spekka-docs/src/main/scala/PartitionAutoBaseExample.scala) { #flow-definition}

Now let's use this new flow definition to implement the grouping by deployment:

@@snip[FlowDefinition.scala](/spekka-docs/src/main/scala/PartitionAutoBaseExample.scala) { #partition-deployment}

Here we define the various partitioning that we want to perform in our partition tree and finally we construct the overall flow invoking the `build` method. This method acts as a kind of factory which will be called when a particular partition needs to be created.

The first thing we can observe is the strange syntax `deployment :@: KNil` which is used to expose the typed sequence of keys of the whole partition tree (it is like a list but it can have heterogeneous types).

@@@note
The order of the keys is inverse with respect to the order in which we partitioned the stream.
This means that is we first partition by `key1` of type `Key1` and then again by `key2` of type `Key2`, we will have the following keys list in the `build` method: `(key2: Key2) :@: (key1: Key1) :@: KNil`.
@@@

If we were to inspect the type of `totalByDeploymentFlow` we would see the materialized value is: `PartitionControl.DynamicControl[DeploymentId,AtomicReference[Int]]`.

This control object can be used carries along the key type of the our partitioning (i.e. `DeploymentId`) and also the type of the materialization of the single substreams (i.e. `AtomicReference[Int]`). All of this is wrapped in a control object which we can uso to:

- get the materialization value of a particular key
- request that the flow for a particular key is terminated/created

Now that we have our partitioned flow we can materialize the stream:

@@snip[FlowMaterialization.scala](/spekka-docs/src/main/scala/PartitionAutoBaseExample.scala) { #stream-materialization }

Notice that now that we are partitioning the data, we lost the ordering property, meaning that we might end up committing offset too early with respect to what has been actually processed.

`FlowWithExtendedContext` offers the `ordered` operator which seamlessly re-order the flow maintaining back-pressure (i.e. no unbounded memory usage).

At this point we can query a particular partition of the stream using the control object we have materialized:

@@snip[FlowQuery.scala](/spekka-docs/src/main/scala/PartitionAutoBaseExample.scala) { #stream-query }

You can find the full example here: @github[PartitionAutoBaseExample.scala](/spekka-docs/src/main/scala/PartitionAutoBaseExample.scala).

## Static partitioning

It often happens that we want to partition our stream in a more static way. Akka offer the `GraphDSL` with the `Partition`, `Broadcast` and `Merge` stages which we can use to achieve this goal. Working with such a low level API however is somewhat difficult and definitely too verbose. Furthermore we are left to our own devices for what concerns context propagation.

Spekka offers a seamless way to define static partition using the same `PartitionTree` API.

@@@note
The term *partition* is not actually correct for the functionality Spekka provides.
When one think about partitioning data, one expect that each element is assigned to exactly one partition. Spekka supports *mutlicast*, meaning that a single element may be assigned to some or all existing partitions with proper context propagation.

All the different partition types offered by the `PartitionTree` API are also available in *multicast* version by suffixing the method name with `Multicast`.
@@@

Let's say that we are not content with having just the counter by deployment but we would also like to know how much the various entrances are used.

Similarly to what we did before we can build our dynamic partition tree to do this:

@@snip[FlowDefinition.scala](/spekka-docs/src/main/scala/PartitionAutoExample.scala) { #partition-entrance}

Now we just need to combine the 2 flows into a single flow performing both the computations:

@@snip[FlowDefinition.scala](/spekka-docs/src/main/scala/PartitionAutoExample.scala) { #partition-combination}

Now that we have our combined flow we can materialize the stream:

@@snip[FlowMaterialization.scala](/spekka-docs/src/main/scala/PartitionAutoExample.scala) { #stream-materialization }

At this point we can query both for deployment and entrances:

@@snip[FlowQuery.scala](/spekka-docs/src/main/scala/PartitionAutoExample.scala) { #stream-query }

You can find the full example here: @github[PartitionAutoExample.scala](/spekka-docs/src/main/scala/PartitionAutoExample.scala).