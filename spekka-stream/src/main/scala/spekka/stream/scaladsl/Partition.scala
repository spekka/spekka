/*
 * Copyright 2022 Andrea Zito
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package spekka.stream.scaladsl

import akka.Done
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.GraphDSL
import akka.stream.stage.AsyncCallback
import spekka.stream.ExtendedContext
import spekka.stream.scaladsl.internal.Multiplexed
import spekka.stream.scaladsl.internal.PartitionDynamicInternal
import spekka.stream.scaladsl.internal.PartitionStaticInternal

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration.FiniteDuration

object PartitionDynamic {
  val defaultBufferSize: Int = 128
  def defaultCompletionCriteria[Ctx]: CompletionCriteria[Any, Any, Ctx] =
    CompletionCriteria.never[Ctx]

  /** Completion criteria.
    *
    * Each partition handler can be automatically completed based on:
    *   - input value
    *   - output value
    *   - idle timeout since the last received input
    */
  case class CompletionCriteria[-In, -Out, Ctx] private (
      completeOnInput: Option[(In, Ctx) => Boolean],
      completeOnOutput: Option[(Out, Ctx) => Boolean],
      completeOnIdle: Option[FiniteDuration]) {
    private[stream] def shouldCompleteOnInput(in: In, ctx: ExtendedContext[Ctx]): Boolean =
      completeOnInput.map(_.apply(in, ctx.innerContext)).getOrElse(false)
    private[stream] def shouldCompleteOnOutput(out: Out, ctx: ExtendedContext[Ctx]): Boolean =
      completeOnOutput.map(_.apply(out, ctx.innerContext)).getOrElse(false)
  }

  object CompletionCriteria {

    /** Partition handlers will never be completed automatically
      */
    def never[Ctx]: CompletionCriteria[Any, Any, Ctx] = CompletionCriteria(None, None, None)

    /** Partition handlers will be completed when the input value satisfies the predicate
      */
    def onInput[In, Ctx](f: In => Boolean): CompletionCriteria[In, Any, Ctx] =
      CompletionCriteria(Some((in: In, _: Any) => f(in)), None, None)

    /** Partition handlers will be completed when the input value satisfies the predicate
      */
    def onInputWithContext[In, Ctx](
        f: (In, Ctx) => Boolean
      ): CompletionCriteria[In, Any, Ctx] = CompletionCriteria(Some(f), None, None)

    /** Partition handlers will be completed when the output value satisfies the predicate
      */
    def onOutput[Out, Ctx](f: Out => Boolean): CompletionCriteria[Any, Out, Ctx] =
      CompletionCriteria(None, Some((out: Out, _: Any) => f(out)), None)

    /** Partition handlers will be completed when the output value satisfies the predicate
      */
    def onOutputWithContext[Out, Ctx](
        f: (Out, Ctx) => Boolean
      ): CompletionCriteria[Any, Out, Ctx] = CompletionCriteria(None, Some(f), None)

    /** Partition handlers will be completed when no input is received for the specified duration
      */
    def onIdle[Ctx](idleTimeout: FiniteDuration): CompletionCriteria[Any, Any, Ctx] =
      CompletionCriteria(None, None, Some(idleTimeout))
  }

  /** Control interface materialized by [[PartitionDynamic]].
    *
    * It enables controlling the current materialized partitions from outside the stream.
    */
  class Control[K, M](
      getKeysCallback: AsyncCallback[Promise[Set[K]]],
      spawnKeyCallback: AsyncCallback[(K, Promise[M])],
      completeKeyCallback: AsyncCallback[(K, Promise[Done])],
      recreateKeyCallback: AsyncCallback[(K, Promise[(Option[M], M)])],
      withMaterializedValueCallback: AsyncCallback[(K, M => Any, Promise[Option[Any]])]) {

    /** Returns the currently materialized partition keys.
      * @return
      *   Currently materialized partition keys
      */
    def getKeys(): Future[Set[K]] = {
      val p = Promise[Set[K]]()
      getKeysCallback.invoke(p)
      p.future
    }

    /** Materialize the handler for the specified key (if it is not already materialized) and return
      * its materialized value.
      *
      * In case the key is already materialized, the existing materialized value is returned.
      *
      * @param key
      *   The partition key to materialize
      * @return
      *   Future containing the materialization value of the partition handler
      */
    def materializeKey(key: K): Future[M] = {
      val p = Promise[M]()
      spawnKeyCallback.invoke((key, p))
      p.future
    }

    /** Complete the handler for the specified key.
      *
      * The returned future completes as soon as the completion signal is sent to the handler.
      * @param key
      *   The partition key to complete
      * @return
      *   Future indicating the successful initiation of the completion process
      */
    def completeKey(key: K): Future[Done] = {
      val p = Promise[Done]()
      completeKeyCallback.invoke((key, p))
      p.future
    }

    /** Completes the current handler for the specified key and re-materialize it.
      *
      * @param key
      *   The partition key to re-materialize
      * @return
      *   Future with old (if present) and new materialized value
      */
    def rematerializeKey(key: K): Future[(Option[M], M)] = {
      val p = Promise[(Option[M], M)]()
      recreateKeyCallback.invoke((key, p))
      p.future
    }

    /** Allows to synchronously apply a function on the materialized value of the specified key. As
      * long as the function is running, the handler is guaranteed not to complete and to be the
      * currently active one.
      *
      * @param key
      *   The partition key of the target materialized value
      * @param f
      *   The function to be performed on the materialized value
      * @return
      *   Future containing the application of the function on the materialized value or None if the
      *   specified key is not currently materialized.
      */
    def withKeyMaterializedValue[U](key: K)(f: M => U): Future[Option[U]] = {
      val p = Promise[Option[Any]]()
      withMaterializedValueCallback.invoke((key, f.asInstanceOf[M => Any], p))
      p.future.asInstanceOf[Future[Option[U]]]
    }
  }
}

object Partition {
  import FlowWithExtendedContext.syntax._

  /** Returns a builder which can be used to create a partition tree for the given input and context
    * types
    *
    * @return
    *   Partition tree builder
    */
  def treeBuilder[In, Ctx]: PartitionTreeBuilder[In, Ctx]#Root =
    new PartitionTreeBuilder[In, Ctx].Root

  /** Creates a dynamic partitioning flow with unicast distribution logic and automatic partition
    * handler materialization.
    *
    * @param partitionKeyF
    *   Extract the partition identifier from the input
    * @param partitionFlowF
    *   Factory of handlers flow parametrized by the partition key
    * @param completionCriteria
    *   Completion criteria for the handlers
    * @param bufferSize
    *   Buffer size for each partition
    * @return
    *   Dynamically partitioned unicast flow with automatic handler materialization
    */
  def dynamic[In, Out, Ctx, K, M](
      partitionKeyF: (In, Ctx) => K,
      partitionFlowF: K => FlowWithExtendedContext[In, Out, Ctx, M],
      completionCriteria: PartitionDynamic.CompletionCriteria[In, Out, Ctx] =
        PartitionDynamic.defaultCompletionCriteria,
      bufferSize: Int = PartitionDynamic.defaultBufferSize
    ): FlowWithExtendedContext[In, Out, Ctx, PartitionDynamic.Control[K, M]] = {
    FlowWithExtendedContext.fromGraphUnsafe(
      new PartitionDynamicInternal.PartitionStage[K, In, Out, Ctx, M](
        PartitionDynamicInternal.Partitioner.SinglePartitioner(
          partitionKeyF,
          partitionFlowF,
          FlowWithExtendedContext[In, Ctx].map(_ =>
            throw new IllegalStateException(
              "Passthrough flow should'nt be required for single partitioned dynamic with auto key creation!"
            )
          )
        ),
        completionCriteria,
        true,
        bufferSize,
        Set.empty
      )
    )
  }

  /** Creates a dynamic partitioning flow with unicast distribution logic and manual partition
    * handler materialization.
    *
    * Partition handlers can materialized/completed using the materialized [[Control]] interface.
    *
    * Inputs destined to non materialized partition are instead routed through the
    * `passthroughFlow`.
    *
    * @param partitionKeyF
    *   Extract the partition identifier from the input
    * @param partitionFlowF
    *   Factory of handlers flow parametrized by the partition key
    * @param passthroughFlow
    *   Flow used for non materialized partitions key
    * @param completionCriteria
    *   Completion criteria for the handlers
    * @param initialKeys
    *   Partition keys to be materialized on startup
    * @param bufferSize
    *   Buffer size for each partition
    * @return
    *   Dynamically partitioned unicast flow with manual handler materialization
    */
  def dynamicManual[In, Out, Ctx, K, M](
      partitionKeyF: (In, Ctx) => K,
      partitionFlowF: K => FlowWithExtendedContext[In, Out, Ctx, M],
      passthroughFlow: FlowWithExtendedContext[In, Out, Ctx, _],
      completionCriteria: PartitionDynamic.CompletionCriteria[In, Out, Ctx] =
        PartitionDynamic.defaultCompletionCriteria,
      initialKeys: Set[K] = Set.empty,
      bufferSize: Int = PartitionDynamic.defaultBufferSize
    ): FlowWithExtendedContext[In, Out, Ctx, PartitionDynamic.Control[K, M]] = {
    FlowWithExtendedContext.fromGraphUnsafe(
      new PartitionDynamicInternal.PartitionStage[K, In, Out, Ctx, M](
        PartitionDynamicInternal.Partitioner
          .SinglePartitioner(partitionKeyF, partitionFlowF, passthroughFlow),
        completionCriteria,
        false,
        bufferSize,
        initialKeys
      )
    )
  }

  /** Creates a dynamic partitioning flow with multicast distribution logic and automatic partition
    * handler materialization.
    *
    * The `partitionKeyF` function is given the set of currently materialized partition keys in
    * order to broadcast a message to all materialized flows.
    *
    * @param partitionKeyF
    *   Extract the partition identifiers from the input
    * @param partitionFlowF
    *   Factory of handlers flow parametrized by the partition key
    * @param passthroughFlow
    *   Flow used for inputs not assigned to any key
    * @param completionCriteria
    *   Completion criteria for the handlers
    * @param bufferSize
    *   Buffer size for each partition
    * @return
    *   Dynamically partitioned multicast flow with automatic handler materialization
    */
  def dynamicMulti[In, Out, Ctx, K, M](
      partitionKeyF: (In, Ctx, Set[K]) => Set[K],
      partitionFlowF: K => FlowWithExtendedContext[In, Out, Ctx, M],
      passthroughFlow: FlowWithExtendedContext[In, Out, Ctx, _],
      completionCriteria: PartitionDynamic.CompletionCriteria[In, Out, Ctx] =
        PartitionDynamic.defaultCompletionCriteria,
      bufferSize: Int = PartitionDynamic.defaultBufferSize
    ): FlowWithExtendedContext[In, immutable.Iterable[Out], Ctx, PartitionDynamic.Control[K, M]] = {
    import FlowWithExtendedContext.syntax._
    Flow
      .fromGraph(
        new PartitionDynamicInternal.PartitionStage[K, In, Out, Ctx, M](
          PartitionDynamicInternal.Partitioner.MultiPartitioner(
            partitionKeyF,
            partitionFlowF,
            passthroughFlow
          ),
          completionCriteria,
          true,
          bufferSize,
          Set.empty
        )
      )
      .via(new Multiplexed.UnorderedMultiplexStage[Out, Ctx])
      .asFlowWithPreservedContextUnsafe
  }

  /** Creates a dynamic partitioning flow with multicast distribution logic and manual partition
    * handler materialization.
    *
    * Partition handlers can materialized/completed using the materialized [[Control]] interface.
    *
    * Inputs destined to non materialized partition are instead routed through the
    * `passthroughFlow`.
    *
    * The `partitionKeyF` function is given the set of currently materialized partition keys in
    * order to broadcast a message to all materialized flows.
    *
    * @param partitionKeyF
    *   Extract the partition identifiers from the input
    * @param partitionFlowF
    *   Factory of handlers flow parametrized by the partition key
    * @param passthroughFlow
    *   Flow used for non materialized partitions key
    * @param completionCriteria
    *   Completion criteria for the handlers
    * @param bufferSize
    *   Buffer size for each partition
    * @param initialKeys
    *   Partition keys to be materialized on startup
    * @return
    *   Dynamically partitioned multicast flow with manual handler materialization
    */
  def dynamicMultiManual[In, Out, Ctx, K, M](
      partitionKeyF: (In, Ctx, Set[K]) => Set[K],
      partitionFlowF: K => FlowWithExtendedContext[In, Out, Ctx, M],
      passthroughFlow: FlowWithExtendedContext[In, Out, Ctx, _],
      completionCriteria: PartitionDynamic.CompletionCriteria[In, Out, Ctx] =
        PartitionDynamic.defaultCompletionCriteria,
      initialKeys: Set[K] = Set.empty,
      bufferSize: Int = PartitionDynamic.defaultBufferSize
    ): FlowWithExtendedContext[In, immutable.Iterable[Out], Ctx, PartitionDynamic.Control[K, M]] = {
    import FlowWithExtendedContext.syntax._
    Flow
      .fromGraph(
        new PartitionDynamicInternal.PartitionStage[K, In, Out, Ctx, M](
          PartitionDynamicInternal.Partitioner.MultiPartitioner(
            partitionKeyF,
            partitionFlowF,
            passthroughFlow
          ),
          completionCriteria,
          false,
          bufferSize,
          initialKeys
        )
      )
      .via(new Multiplexed.UnorderedMultiplexStage[Out, Ctx])
      .asFlowWithPreservedContextUnsafe
  }

  /** Creates a static partitioning flow with unicast distribution logic and homogeneous flows.
    *
    * @param partitionKeyF
    *   Extract the partition identifier from the input
    * @param flowByKey
    *   Partition flow association map
    * @param passthroughFlow
    *   Flow used for keys not specified in the association map
    * @return
    *   Statically partitioned unicast flow
    */
  def static[In, Out, Ctx, K, M](
      partitionKeyF: (In, Ctx) => K,
      flowByKey: Map[K, FlowWithExtendedContext[In, Out, Ctx, M]],
      passthroughFlow: FlowWithExtendedContext[In, Out, Ctx, _]
    ): FlowWithExtendedContext[In, Out, Ctx, Map[K, M]] = {
    val graphsSeq = flowByKey.iterator.map { case (k, flow) => k -> flow.toGraph }.toList

    Flow
      .fromGraph(
        GraphDSL.create(graphsSeq.map(_._2)) { implicit builder => shapesSeq =>
          val shapesByKey = shapesSeq
            .zip(graphsSeq)
            .map { case (flow, (key, _)) =>
              key -> flow
            }
            .toMap

          val pfu = builder.add(passthroughFlow.toGraph)
          PartitionStaticInternal.buildSingle[In, Out, Ctx, K](
            (in, ctx) => partitionKeyF(in, ctx.innerContext),
            shapesByKey,
            pfu
          )
        }
      )
      .mapMaterializedValue(_.zip(graphsSeq).map { case (m, (k, _)) => k -> m }.toMap)
      .asFlowWithPreservedContextUnsafe
  }

  /** Creates a static partitioning flow with unicast distribution logic and heterogeneous flows.
    *
    * @param partitionKeyF
    *   Extract the partition identifier from the input
    * @param passthroughFlow
    *   Flow used for keys not specified in the association map
    * @param p1
    *   Key and flow for partition 1
    * @param p2
    *   Key and flow for partition 2
    * @return
    *   Statically partitioned unicast flow
    */
  def staticMat[In, Out, Ctx, K, M1, M2, MU](
      partitionKeyF: (In, Ctx) => K,
      passthroughFlow: FlowWithExtendedContext[In, Out, Ctx, MU]
    )(p1: (K, FlowWithExtendedContext[In, Out, Ctx, M1]),
      p2: (K, FlowWithExtendedContext[In, Out, Ctx, M2])
    ): FlowWithExtendedContext[In, Out, Ctx, (M1, M2, MU)] = {
    require(Set(p1._1, p2._1).size == 2, "Partition keys must be unique")
    Flow
      .fromGraph(
        GraphDSL.createGraph(p1._2.toGraph, p2._2.toGraph, passthroughFlow.toGraph)((m1, m2, mu) =>
          (m1, m2, mu)
        ) { implicit builder =>
          { case (pf1, pf2, pfu) =>
            PartitionStaticInternal.buildSingle[In, Out, Ctx, K](
              (in, ctx) => partitionKeyF(in, ctx.innerContext),
              Map(p1._1 -> pf1, p2._1 -> pf2),
              pfu
            )
          }
        }
      )
      .asFlowWithPreservedContextUnsafe
  }

  /** Creates a static partitioning flow with unicast distribution logic and heterogeneous flows.
    *
    * @param partitionKeyF
    *   Extract the partition identifier from the input
    * @param passthroughFlow
    *   Flow used for keys not specified in the association map
    * @param p1
    *   Key and flow for partition 1
    * @param p2
    *   Key and flow for partition 2
    * @param p3
    *   Key and flow for partition 3
    * @return
    *   Statically partitioned unicast flow
    */
  def staticMat[In, Out, Ctx, K, M1, M2, M3, MU](
      partitionKeyF: (In, Ctx) => K,
      passthroughFlow: FlowWithExtendedContext[In, Out, Ctx, MU]
    )(p1: (K, FlowWithExtendedContext[In, Out, Ctx, M1]),
      p2: (K, FlowWithExtendedContext[In, Out, Ctx, M2]),
      p3: (K, FlowWithExtendedContext[In, Out, Ctx, M3])
    ): FlowWithExtendedContext[In, Out, Ctx, (M1, M2, M3, MU)] = {
    require(Set(p1._1, p2._1, p3._1).size == 3, "Partition keys must be unique")
    Flow
      .fromGraph(
        GraphDSL.createGraph(p1._2.toGraph, p2._2.toGraph, p3._2.toGraph, passthroughFlow.toGraph)(
          (m1, m2, m3, mu) => (m1, m2, m3, mu)
        ) { implicit builder =>
          { case (pf1, pf2, pf3, pfu) =>
            PartitionStaticInternal.buildSingle[In, Out, Ctx, K](
              (in, ctx) => partitionKeyF(in, ctx.innerContext),
              Map(p1._1 -> pf1, p2._1 -> pf2, p3._1 -> pf3),
              pfu
            )
          }
        }
      )
      .asFlowWithPreservedContextUnsafe
  }

  /** Creates a static partitioning flow with unicast distribution logic and heterogeneous flows.
    *
    * @param partitionKeyF
    *   Extract the partition identifier from the input
    * @param passthroughFlow
    *   Flow used for keys not specified in the association map
    * @param p1
    *   Key and flow for partition 1
    * @param p2
    *   Key and flow for partition 2
    * @param p3
    *   Key and flow for partition 3
    * @param p4
    *   Key and flow for partition 4
    * @return
    *   Statically partitioned unicast flow
    */
  def staticMat[In, Out, Ctx, K, M1, M2, M3, M4, MU](
      partitionKeyF: (In, Ctx) => K,
      passthroughFlow: FlowWithExtendedContext[In, Out, Ctx, MU]
    )(p1: (K, FlowWithExtendedContext[In, Out, Ctx, M1]),
      p2: (K, FlowWithExtendedContext[In, Out, Ctx, M2]),
      p3: (K, FlowWithExtendedContext[In, Out, Ctx, M3]),
      p4: (K, FlowWithExtendedContext[In, Out, Ctx, M4])
    ): FlowWithExtendedContext[In, Out, Ctx, (M1, M2, M3, M4, MU)] = {
    require(Set(p1._1, p2._1, p3._1, p4._1).size == 4, "Partition keys must be unique")
    Flow
      .fromGraph(
        GraphDSL.createGraph(
          p1._2.toGraph,
          p2._2.toGraph,
          p3._2.toGraph,
          p4._2.toGraph,
          passthroughFlow.toGraph
        )((m1, m2, m3, m4, mu) => (m1, m2, m3, m4, mu)) { implicit builder =>
          { case (pf1, pf2, pf3, pf4, pfu) =>
            PartitionStaticInternal.buildSingle[In, Out, Ctx, K](
              (in, ctx) => partitionKeyF(in, ctx.innerContext),
              Map(p1._1 -> pf1, p2._1 -> pf2, p3._1 -> pf3, p4._1 -> pf4),
              pfu
            )
          }
        }
      )
      .asFlowWithPreservedContextUnsafe
  }

  /** Creates a static partitioning flow with unicast distribution logic and heterogeneous flows.
    *
    * @param partitionKeyF
    *   Extract the partition identifier from the input
    * @param passthroughFlow
    *   Flow used for keys not specified in the association map
    * @param p1
    *   Key and flow for partition 1
    * @param p2
    *   Key and flow for partition 2
    * @param p3
    *   Key and flow for partition 3
    * @param p4
    *   Key and flow for partition 4
    * @param p5
    *   Key and flow for partition 5
    * @return
    *   Statically partitioned unicast flow
    */
  def staticMat[In, Out, Ctx, K, M1, M2, M3, M4, M5, MU](
      partitionKeyF: (In, Ctx) => K,
      passthroughFlow: FlowWithExtendedContext[In, Out, Ctx, MU]
    )(p1: (K, FlowWithExtendedContext[In, Out, Ctx, M1]),
      p2: (K, FlowWithExtendedContext[In, Out, Ctx, M2]),
      p3: (K, FlowWithExtendedContext[In, Out, Ctx, M3]),
      p4: (K, FlowWithExtendedContext[In, Out, Ctx, M4]),
      p5: (K, FlowWithExtendedContext[In, Out, Ctx, M5])
    ): FlowWithExtendedContext[In, Out, Ctx, (M1, M2, M3, M4, M5, MU)] = {
    require(Set(p1._1, p2._1, p3._1, p4._1, p5._1).size == 5, "Partition keys must be unique")
    Flow
      .fromGraph(
        GraphDSL.createGraph(
          p1._2.toGraph,
          p2._2.toGraph,
          p3._2.toGraph,
          p4._2.toGraph,
          p5._2.toGraph,
          passthroughFlow.toGraph
        )((m1, m2, m3, m4, m5, mu) => (m1, m2, m3, m4, m5, mu)) { implicit builder =>
          { case (pf1, pf2, pf3, pf4, pf5, pfu) =>
            PartitionStaticInternal.buildSingle[In, Out, Ctx, K](
              (in, ctx) => partitionKeyF(in, ctx.innerContext),
              Map(p1._1 -> pf1, p2._1 -> pf2, p3._1 -> pf3, p4._1 -> pf4, p5._1 -> pf5),
              pfu
            )
          }
        }
      )
      .asFlowWithPreservedContextUnsafe
  }

  /** Creates a static partitioning flow with multicast distribution logic and homogeneous flows.
    *
    * The `partitionKeyF` function is given the set of currently materialized partition keys in
    * order to broadcast a message to all materialized flows.
    *
    * @param partitionKeyF
    *   Extract the partition identifiers from the input
    * @param flowByKey
    *   Partition flow association map
    * @param passthroughFlow
    *   Flow used for keys not specified in the association map
    * @return
    *   Statically partitioned multicast flow
    */
  def staticMulti[In, Out, Ctx, K, M](
      partitionKeyF: (In, Ctx, Set[K]) => Set[K],
      flowByKey: Map[K, FlowWithExtendedContext[In, Out, Ctx, M]],
      passthroughFlow: FlowWithExtendedContext[In, Out, Ctx, _]
    ): FlowWithExtendedContext[In, immutable.Iterable[Out], Ctx, Map[K, M]] = {
    val graphsSeq = flowByKey.iterator.map { case (k, flow) => k -> flow.toGraph }.toList

    Flow
      .fromGraph(
        GraphDSL.create(graphsSeq.map(_._2)) { implicit builder => shapesSeq =>
          val shapesByKey = shapesSeq
            .zip(graphsSeq)
            .map { case (flow, (key, _)) =>
              key -> flow
            }
            .toMap

          val pfu = builder.add(passthroughFlow.toGraph)
          PartitionStaticInternal.buildMulti[In, Out, Ctx, K](partitionKeyF, shapesByKey, pfu)
        }
      )
      .mapMaterializedValue(_.zip(graphsSeq).map { case (m, (k, _)) => k -> m }.toMap)
      .asFlowWithPreservedContextUnsafe
  }

  /** Creates a static partitioning flow with multicast distribution logic and heterogeneous flows.
    *
    * @param partitionKeyF
    *   Extract the partition identifier from the input
    * @param passthroughFlow
    *   Flow used for keys not specified in the association map
    * @param p1
    *   Key and flow for partition 1
    * @param p2
    *   Key and flow for partition 2
    * @return
    *   Statically partitioned unicast flow
    */
  def staticMultiMat[In, Out, Ctx, K, M1, M2, MU](
      partitionKeyF: (In, Ctx, Set[K]) => Set[K],
      passthroughFlow: FlowWithExtendedContext[In, Out, Ctx, MU]
    )(p1: (K, FlowWithExtendedContext[In, Out, Ctx, M1]),
      p2: (K, FlowWithExtendedContext[In, Out, Ctx, M2])
    ): FlowWithExtendedContext[In, immutable.Iterable[Out], Ctx, (M1, M2, MU)] = {
    require(Set(p1._1, p2._1).size == 2, "Partition keys must be unique")
    Flow
      .fromGraph(
        GraphDSL.createGraph(p1._2.toGraph, p2._2.toGraph, passthroughFlow.toGraph)((m1, m2, mu) =>
          (m1, m2, mu)
        ) { implicit builder =>
          { case (pf1, pf2, pfu) =>
            PartitionStaticInternal
              .buildMulti[In, Out, Ctx, K](partitionKeyF, Map(p1._1 -> pf1, p2._1 -> pf2), pfu)
          }
        }
      )
      .asFlowWithPreservedContextUnsafe
  }

  /** Creates a static partitioning flow with multicast distribution logic and heterogeneous flows.
    *
    * @param partitionKeyF
    *   Extract the partition identifier from the input
    * @param passthroughFlow
    *   Flow used for keys not specified in the association map
    * @param p1
    *   Key and flow for partition 1
    * @param p2
    *   Key and flow for partition 2
    * @param p3
    *   Key and flow for partition 3
    * @return
    *   Statically partitioned unicast flow
    */
  def staticMultiMat[In, Out, Ctx, K, M1, M2, M3, MU](
      partitionKeyF: (In, Ctx, Set[K]) => Set[K],
      passthroughFlow: FlowWithExtendedContext[In, Out, Ctx, MU]
    )(p1: (K, FlowWithExtendedContext[In, Out, Ctx, M1]),
      p2: (K, FlowWithExtendedContext[In, Out, Ctx, M2]),
      p3: (K, FlowWithExtendedContext[In, Out, Ctx, M3])
    ): FlowWithExtendedContext[In, immutable.Iterable[Out], Ctx, (M1, M2, M3, MU)] = {
    require(Set(p1._1, p2._1, p3._1).size == 3, "Partition keys must be unique")
    Flow
      .fromGraph(
        GraphDSL.createGraph(p1._2.toGraph, p2._2.toGraph, p3._2.toGraph, passthroughFlow.toGraph)(
          (m1, m2, m3, mu) => (m1, m2, m3, mu)
        ) { implicit builder =>
          { case (pf1, pf2, pf3, pfu) =>
            PartitionStaticInternal.buildMulti[In, Out, Ctx, K](
              partitionKeyF,
              Map(p1._1 -> pf1, p2._1 -> pf2, p3._1 -> pf3),
              pfu
            )
          }
        }
      )
      .asFlowWithPreservedContextUnsafe
  }

  /** Creates a static partitioning flow with multicast distribution logic and heterogeneous flows.
    *
    * @param partitionKeyF
    *   Extract the partition identifier from the input
    * @param passthroughFlow
    *   Flow used for keys not specified in the association map
    * @param p1
    *   Key and flow for partition 1
    * @param p2
    *   Key and flow for partition 2
    * @param p3
    *   Key and flow for partition 3
    * @param p4
    *   Key and flow for partition 4
    * @return
    *   Statically partitioned unicast flow
    */
  def staticMultiMat[In, Out, Ctx, K, M1, M2, M3, M4, MU](
      partitionKeyF: (In, Ctx, Set[K]) => Set[K],
      passthroughFlow: FlowWithExtendedContext[In, Out, Ctx, MU]
    )(p1: (K, FlowWithExtendedContext[In, Out, Ctx, M1]),
      p2: (K, FlowWithExtendedContext[In, Out, Ctx, M2]),
      p3: (K, FlowWithExtendedContext[In, Out, Ctx, M3]),
      p4: (K, FlowWithExtendedContext[In, Out, Ctx, M4])
    ): FlowWithExtendedContext[In, immutable.Iterable[Out], Ctx, (M1, M2, M3, M4, MU)] = {
    require(Set(p1._1, p2._1, p3._1, p4._1).size == 4, "Partition keys must be unique")
    Flow
      .fromGraph(
        GraphDSL.createGraph(
          p1._2.toGraph,
          p2._2.toGraph,
          p3._2.toGraph,
          p4._2.toGraph,
          passthroughFlow.toGraph
        )((m1, m2, m3, m4, mu) => (m1, m2, m3, m4, mu)) { implicit builder =>
          { case (pf1, pf2, pf3, pf4, pfu) =>
            PartitionStaticInternal.buildMulti[In, Out, Ctx, K](
              partitionKeyF,
              Map(p1._1 -> pf1, p2._1 -> pf2, p3._1 -> pf3, p4._1 -> pf4),
              pfu
            )
          }
        }
      )
      .asFlowWithPreservedContextUnsafe
  }

  /** Creates a static partitioning flow with multicast distribution logic and heterogeneous flows.
    *
    * @param partitionKeyF
    *   Extract the partition identifier from the input
    * @param passthroughFlow
    *   Flow used for keys not specified in the association map
    * @param p1
    *   Key and flow for partition 1
    * @param p2
    *   Key and flow for partition 2
    * @param p3
    *   Key and flow for partition 3
    * @param p4
    *   Key and flow for partition 4
    * @param p5
    *   Key and flow for partition 5
    * @return
    *   Statically partitioned unicast flow
    */
  def staticMultiMat[In, Out, Ctx, K, M1, M2, M3, M4, M5, MU](
      partitionKeyF: (In, Ctx, Set[K]) => Set[K],
      passthroughFlow: FlowWithExtendedContext[In, Out, Ctx, MU]
    )(p1: (K, FlowWithExtendedContext[In, Out, Ctx, M1]),
      p2: (K, FlowWithExtendedContext[In, Out, Ctx, M2]),
      p3: (K, FlowWithExtendedContext[In, Out, Ctx, M3]),
      p4: (K, FlowWithExtendedContext[In, Out, Ctx, M4]),
      p5: (K, FlowWithExtendedContext[In, Out, Ctx, M5])
    ): FlowWithExtendedContext[In, immutable.Iterable[Out], Ctx, (M1, M2, M3, M4, M5, MU)] = {
    require(Set(p1._1, p2._1, p3._1, p4._1, p5._1).size == 5, "Partition keys must be unique")
    Flow
      .fromGraph(
        GraphDSL.createGraph(
          p1._2.toGraph,
          p2._2.toGraph,
          p3._2.toGraph,
          p4._2.toGraph,
          p5._2.toGraph,
          passthroughFlow.toGraph
        )((m1, m2, m3, m4, m5, mu) => (m1, m2, m3, m4, m5, mu)) { implicit builder =>
          { case (pf1, pf2, pf3, pf4, pf5, pfu) =>
            PartitionStaticInternal.buildMulti[In, Out, Ctx, K](
              partitionKeyF,
              Map(p1._1 -> pf1, p2._1 -> pf2, p3._1 -> pf3, p4._1 -> pf4, p5._1 -> pf5),
              pfu
            )
          }
        }
      )
      .asFlowWithPreservedContextUnsafe
  }
}
