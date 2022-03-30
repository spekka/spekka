# Partitions

`FlowWithExtendedContext` are a good abstraction to model *one-to-one* streams, however by themselves they are not enough.

In the @ref:[Flow With Extended Context](./flow-with-extended-context.md) section we showed how to create a stream which computes the total number of people entered in a store. The problem as presented was so simple that a `FlowWithExtendedContext` was not actually needed. A standard `FlowWithContext` would have be more than enough.

The benefit introduced by `FlowWithExtendedContext` becomes more apparent as soon as we start making the stream a little bit more complicated.

For starters, let's say that we would like to support more than a single deployment. 

We could embed the logic required for handling multiple deployments in the flow itself and while this would technically work, it would require to complicate the *business logic*.

An alternative approach would be to use Akka's `SubStream` to partition the stream by deployment and then use the same logic. This approach is definitely more in line with what we would like to achieve however it comes with some disadvantages:
- we cannot access the materialization value of subflows
- we have limited control in how the partitions are managed
- by design Akka sub stream are subject to race-conditions around sub-flow completion and re-materialization (see [Akka's groupBy documentation](https://doc.akka.io/docs/akka/current/stream/operators/Source-or-Flow/groupBy.html)) which may cause stream elements to be lost

As partitioning data is such a common task, Spekka provides an extensive support for partition via the `Partition` object.

In particular, the standard way to use Spekka's partition support should be to always start
with the following code:

```scala
import PartitionTree._
Partition.treeBuilder[Input, Context]
```

## Dynamic partitioning with automatic materialization

The equivalent of Akka's `groupBy` is represented by Spekka dynamic partition with automatic materialization. This kind of partition works on a dynamic set of keys for which flows are materialized automatically at the first key occurrence.

This functionality is exposed in the `PartitionTree` API via the `dynamicAuto` method which, in the simples form, takes as a parameter a function used to extract the *partition key* from each input element.

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

At this point we can query a particular partition of the stream for the current counter value by using the control object we have materialized:

@@snip[FlowQuery.scala](/spekka-docs/src/main/scala/PartitionAutoBaseExample.scala) { #stream-query }

You can find the full example here: @github[PartitionAutoBaseExample.scala](/spekka-docs/src/main/scala/PartitionAutoBaseExample.scala).

## Static partitioning

It often happens that we want to partition our stream in a more static way. Akka offer the `GraphDSL` with the `Partition`, `Broadcast` and `Merge` stages which we can use to achieve this goal. Working with such a low level API however is somewhat difficult and definitely too verbose. Furthermore we are left to our own devices for what concerns context propagation.

Spekka offers a seamless way to define static partition using the same `PartitionTree` API via the `static` method.

As for the dynamic case the methods takes as argument a function to extract the *partition key* from each input element, but now we also have to enumerate all the possible *partition keys* in which we plan to partition the data. 

When a flow containing a static partition is built, Spekka will immediately materialize a subflow for each key.

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

Since the subtrees have different shape, we encapsulate the materialized values into a custom type `CombinedMaterialization` to preserve their specific type.

Now that we have our combined flow we can materialize the stream:

@@snip[FlowMaterialization.scala](/spekka-docs/src/main/scala/PartitionAutoExample.scala) { #stream-materialization }

At this point we can query the current counter value for for a particular deployment or for a specific entrance.

@@snip[FlowQuery.scala](/spekka-docs/src/main/scala/PartitionAutoExample.scala) { #stream-query }

You can find the full example here: @github[PartitionAutoExample.scala](/spekka-docs/src/main/scala/PartitionAutoExample.scala).

## Dynamic partitioning with manual materialization

The last kind of partition scheme provided by the `PartitionTree` API is the one of dynamic partitioning with manual materialization. In this scheme the stream is divided into dynamic partitions, however a partition is materialized only by an explicit external command. Input associated to non-materialized partitions will simply propagate the context without producing any effect.

We can use this scheme to implement a new feature in our people counting system: entrance based counters will only be available to those deployment on the *pro* license.

In order to do this we have toi change the definition of our `totalByEntranceFlow`:

@@snip[FlowDefinition.scala](/spekka-docs/src/main/scala/PartitionManualExample.scala) { #partition-entrance}

We define the deployment partitioning layer as *manual*, specifying that the set of deployment that should be materialized at the start of the stream is equal to the empty set (i.e. all deployment starts on the *basic* license).

We then need to define a function that we can call whenever a customer buys a *pro* license for a deployment:

@@snip[StartProcessingDeployment.scala](/spekka-docs/src/main/scala/PartitionManualExample.scala) { #start-processing}

And we're done. At this point only the deployment for which we have invoked the `startCountingByEntranceFor` function will be computing the counters for each individual entrance, while all other deployments will only have the overall count.

You can find the full example here: @github[PartitionManulExample.scala](/spekka-docs/src/main/scala/PartitionManualExample.scala).

## Dynamic partitioning completion

Every time a dynamic partition is materialized, a bunch of memory is allocated both by Spekka's substream handling internal as well as the user code (i.e. in the people counting example a separate counter is kept for each deployment and deployment entrance).

In a long running process this monotonic increase in memory usage could cause the process to crash due to an out of memory error.

To avoid this situation Spekka provide a way to gracefully complete dynamically materialized partitions via the concept of `CompletionCriteria`.

A `CompletionCriteria` instruct Spekka when to complete a materialized substream for a partition reclaiming the allocated memory using the following criteria:

- A predicate expression on the substream input
- A predicate expression on the substream output
- A timeout an the inactivity of the partition (i.e. amount of time without new inputs)

Going back to our example we could force one of the deployment to be garbage collected the moment it stopped receiving data:

@@snip[SubstreamCompletion.scala](/spekka-docs/src/main/scala/PartitionAutoCompletionExample.scala) { #partition-deployment}

You can find the full example here: @github[PartitionAutoCompletionExample.scala](/spekka-docs/src/main/scala/PartitionAutoCompletionExample.scala).
