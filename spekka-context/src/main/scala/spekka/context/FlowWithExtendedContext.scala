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

package spekka.context

import akka.NotUsed
import akka.stream.Attributes
import akka.stream.FlowShape
import akka.stream.Graph
import akka.stream.scaladsl.Broadcast
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.FlowWithContext
import akka.stream.scaladsl.GraphDSL
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Zip
import spekka.context.internal._

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

/** Provides the implicit conversions needed to make [[FlowWithExtendedContext]] interoperatee with
  * standard akka flows.
  */
trait FlowWithExtendedContextSyntax {

  /** Converts the provided [[FlowWithExtendedContext]] into a tupled flow working on the base
    * context.
    * @param flow
    *   the [[FlowWithExtendedContext]] to convert
    * @return
    *   a tupled flow taking/emitting the base context of the [[FlowWithExtendedContext]]
    */
  implicit def toWrappingGraph[In, Out, Ctx, M](
      flow: FlowWithExtendedContext[In, Out, Ctx, M]
    ): Graph[FlowShape[(In, Ctx), (Out, Ctx)], M] = {
    Flow[(In, Ctx)]
      .map { case (out, ctx) => out -> ExtendedContext(ctx) }
      .viaMat(flow.toGraph)(Keep.right)
      .map { case (out, ectx) => out -> ectx.innerContext }
  }

  /** Converts the provided [[FlowWithExtendedContext]] into a tupled flow wotking on the
    * [[ExtendedContext]].
    * @param flow
    *   the [[FlowWithExtendedContext]] to convert
    * @return
    *   a tupled flow taking/emitting the Extended context of the [[FlowWithExtendedContext]]
    */
  implicit def toGraph[In, Out, Ctx, M](
      flow: FlowWithExtendedContext[In, Out, Ctx, M]
    ): Graph[FlowShape[(In, ExtendedContext[Ctx]), (Out, ExtendedContext[Ctx])], M] = {
    flow.toGraph
  }

  /** Ops class for tupled flows using [[ExtendedContext]].
    */
  implicit class GraphConversionOps[In, Out, Ctx, M](
      graph: Graph[FlowShape[(In, ExtendedContext[Ctx]), (Out, ExtendedContext[Ctx])], M]) {

    /** Converts this flow into a [[FlowWithExtendedContext]].
      *
      * Since this flow could potentially perform destructive operations like filtering or
      * duplicating data, this method is marked unsafe.
      *
      * It is the responsibility of the caller to ensure that this flow does adhere to the
      * [[FlowWithExtendedContext]] contract.
      */
    def asFlowWithExtendedContextUnsafe: FlowWithExtendedContext[In, Out, Ctx, M] =
      new FlowWithExtendedContext(graph)
  }

  /** Ops class for [[FlowWithExtendedContext]] with iterable outputs
    */
  implicit class FlowWithExtendedContextListOps[In, Out, Ctx, M](
      backingFlow: FlowWithExtendedContext[In, immutable.Iterable[Out], Ctx, M]) {

    /** Transform this flow by piping each element of the output through the given flow, recombining
      * the results back into a collection.
      *
      * @param flow
      *   the flow used to transform each output elements
      */
    def viaMultiplexed[Out2](
        flow: FlowWithExtendedContext[Out, Out2, Ctx, _]
      ): FlowWithExtendedContext[In, immutable.Iterable[Out2], Ctx, M] =
      backingFlow.via(Multiplexed(flow))

    /** Transform this flow by piping each element of the output through the given flow, recombining
      * the results back into a collection.
      *
      * This variant allows to combine the materialization values.
      *
      * @param flow
      *   the flow used to transform each output elements
      * @param combine
      *   the function used to combine materialization values
      */
    def viaMultiplexedMat[Out2, M2, M3](
        flow: FlowWithExtendedContext[Out, Out2, Ctx, M2]
      )(combine: (M, M2) => M3
      ): FlowWithExtendedContext[In, immutable.Iterable[Out2], Ctx, M3] =
      backingFlow.viaMat(Multiplexed(flow))(combine)
  }
}

object FlowWithExtendedContext extends FlowWithExtendedContextSyntax with FlowWithExtendedContextVersionedExtensions {

  /** Creates a [[FlowWithExtendedContext]] for the given input and context types.
    */
  def apply[In, Ctx]: FlowWithExtendedContext[In, In, Ctx, NotUsed] = {
    val backingFlow = FlowWithContext[In, ExtendedContext[Ctx]]
    new FlowWithExtendedContext[In, In, Ctx, NotUsed](backingFlow)
  }

  /** Wraps a `Flow` into a [[FlowWithExtendedContext]].
    *
    * Since the wrapped flow could potentially perform destructive operations like filtering or
    * duplicating data, this method is marked unsafe.
    *
    * It is the responsibility of the caller to ensure that the wrapped flow does adhere to the
    * [[FlowWithExtendedContext]] contract.
    *
    * @param flow
    *   the flow to wrap into a [[FlowWithExtendedContext]]
    */
  def fromGraphUnsafe[In, Out, Ctx, M](
      graph: Graph[FlowShape[(In, ExtendedContext[Ctx]), (Out, ExtendedContext[Ctx])], M]
    ): FlowWithExtendedContext[In, Out, Ctx, M] = new FlowWithExtendedContext(graph)

  /** Zips two [[FlowWithExtendedContext]] in a single [[FlowWithExtendedContext]] by tupling the
    * outputs and the materialized values.
    *
    * It is the responsibility of the caller to ensure that all the flows adhere to the
    * [[FlowWithExtendedContext]] contract.
    */
  def zip[In, Out1, Out2, Ctx, M1, M2](
      flow1: FlowWithExtendedContext[In, Out1, Ctx, M1],
      flow2: FlowWithExtendedContext[In, Out2, Ctx, M2]
    ): FlowWithExtendedContext[In, (Out1, Out2), Ctx, (M1, M2)] = {
    val zipFlow = Flow.fromGraph(
      GraphDSL.createGraph(flow1.toGraph, flow2.toGraph)((m1, m2) => (m1, m2)) { implicit builder =>
        import GraphDSL.Implicits._
        { case (f1, f2) =>
          val bcast = builder.add(Broadcast[(In, ExtendedContext[Ctx])](2, true))
          val zip = builder.add(Zip[(Out1, ExtendedContext[Ctx]), (Out2, ExtendedContext[Ctx])]())

          bcast.outlets.zip(List(f1, f2)).foreach { case (port, f) =>
            port ~> f
          }

          f1 ~> zip.in0
          f2 ~> zip.in1

          val adapter = builder.add {
            Flow[((Out1, ExtendedContext[Ctx]), (Out2, ExtendedContext[Ctx]))].map {
              case ((o1, ctx), (o2, _)) => (o1, o2) -> ctx
            }
          }

          zip.out ~> adapter

          FlowShape(bcast.in, adapter.out)
        }
      }
    )

    zipFlow.asFlowWithExtendedContextUnsafe
  }

  /** Zips three [[FlowWithExtendedContext]] in a single [[FlowWithExtendedContext]] by tupling the
    * outputs and the materialized values.
    *
    * It is the responsibility of the caller to ensure that all the flows adhere to the
    * [[FlowWithExtendedContext]] contract.
    */
  def zip[In, Out1, Out2, Out3, Ctx, M1, M2, M3](
      flow1: FlowWithExtendedContext[In, Out1, Ctx, M1],
      flow2: FlowWithExtendedContext[In, Out2, Ctx, M2],
      flow3: FlowWithExtendedContext[In, Out3, Ctx, M3]
    ): FlowWithExtendedContext[In, (Out1, Out2, Out3), Ctx, (M1, M2, M3)] = {
    val zipFlow = Flow.fromGraph(
      GraphDSL.createGraph(flow1.toGraph, flow2.toGraph, flow3.toGraph)((m1, m2, m3) =>
        (m1, m2, m3)
      ) { implicit builder =>
        import GraphDSL.Implicits._
        { case (f1, f2, f3) =>
          val bcast = builder.add(Broadcast.apply[(In, ExtendedContext[Ctx])](3, true))

          val zip1 = builder.add(Zip[(Out1, ExtendedContext[Ctx]), (Out2, ExtendedContext[Ctx])]())
          val zip2 = builder.add(
            Zip[
              (
                  (Out1, ExtendedContext[Ctx]),
                  (Out2, ExtendedContext[Ctx])
              ),
              (Out3, ExtendedContext[Ctx])
            ]()
          )

          bcast.outlets.zip(List(f1, f2, f3)).foreach { case (port, f) =>
            port ~> f
          }

          f1 ~> zip1.in0
          f2 ~> zip1.in1

          zip1.out ~> zip2.in0
          f3 ~> zip2.in1

          val adapter = builder.add {
            Flow[
              (
                  ((Out1, ExtendedContext[Ctx]), (Out2, ExtendedContext[Ctx])),
                  (Out3, ExtendedContext[Ctx])
              )
            ].map { case (((o1, ctx), (o2, _)), (o3, _)) =>
              (o1, o2, o3) -> ctx
            }
          }

          zip2.out ~> adapter

          FlowShape(bcast.in, adapter.out)
        }
      }
    )

    zipFlow.asFlowWithExtendedContextUnsafe
  }

  /** Zips four [[FlowWithExtendedContext]] in a single [[FlowWithExtendedContext]] by tupling the
    * outputs and the materialized values.
    *
    * It is the responsibility of the caller to ensure that all the flows adhere to the
    * [[FlowWithExtendedContext]] contract.
    */
  def zip[In, Out1, Out2, Out3, Out4, Ctx, M1, M2, M3, M4](
      flow1: FlowWithExtendedContext[In, Out1, Ctx, M1],
      flow2: FlowWithExtendedContext[In, Out2, Ctx, M2],
      flow3: FlowWithExtendedContext[In, Out3, Ctx, M3],
      flow4: FlowWithExtendedContext[In, Out4, Ctx, M4]
    ): FlowWithExtendedContext[In, (Out1, Out2, Out3, Out4), Ctx, (M1, M2, M3, M4)] = {
    val zipFlow = Flow.fromGraph(
      GraphDSL.createGraph(flow1.toGraph, flow2.toGraph, flow3.toGraph, flow4.toGraph)(
        (m1, m2, m3, m4) => (m1, m2, m3, m4)
      ) { implicit builder =>
        import GraphDSL.Implicits._
        { case (f1, f2, f3, f4) =>
          val bcast = builder.add(Broadcast.apply[(In, ExtendedContext[Ctx])](4, true))

          val zip1 = builder.add(Zip[(Out1, ExtendedContext[Ctx]), (Out2, ExtendedContext[Ctx])]())
          val zip2 = builder.add(
            Zip[
              (
                  (Out1, ExtendedContext[Ctx]),
                  (Out2, ExtendedContext[Ctx])
              ),
              (Out3, ExtendedContext[Ctx])
            ]()
          )
          val zip3 = builder.add(
            Zip[
              (
                  (
                      (Out1, ExtendedContext[Ctx]),
                      (Out2, ExtendedContext[Ctx])
                  ),
                  (Out3, ExtendedContext[Ctx])
              ),
              (Out4, ExtendedContext[Ctx])
            ]()
          )

          bcast.outlets.zip(List(f1, f2, f3, f4)).foreach { case (port, f) =>
            port ~> f
          }

          f1 ~> zip1.in0
          f2 ~> zip1.in1

          zip1.out ~> zip2.in0
          f3 ~> zip2.in1

          zip2.out ~> zip3.in0
          f4 ~> zip3.in1

          val adapter = builder.add {
            Flow[
              (
                  (
                      ((Out1, ExtendedContext[Ctx]), (Out2, ExtendedContext[Ctx])),
                      (Out3, ExtendedContext[Ctx])
                  ),
                  (Out4, ExtendedContext[Ctx])
              )
            ].map { case ((((o1, ctx), (o2, _)), (o3, _)), (o4, _)) =>
              (o1, o2, o3, o4) -> ctx
            }
          }

          zip3.out ~> adapter

          FlowShape(bcast.in, adapter.out)
        }
      }
    )

    zipFlow.asFlowWithExtendedContextUnsafe
  }

  /** Zips five [[FlowWithExtendedContext]] in a single [[FlowWithExtendedContext]] by tupling the
    * outputs and the materialized values.
    *
    * It is the responsibility of the caller to ensure that all the flows adhere to the
    * [[FlowWithExtendedContext]] contract.
    */
  def zip[In, Out1, Out2, Out3, Out4, Out5, Ctx, M1, M2, M3, M4, M5](
      flow1: FlowWithExtendedContext[In, Out1, Ctx, M1],
      flow2: FlowWithExtendedContext[In, Out2, Ctx, M2],
      flow3: FlowWithExtendedContext[In, Out3, Ctx, M3],
      flow4: FlowWithExtendedContext[In, Out4, Ctx, M4],
      flow5: FlowWithExtendedContext[In, Out5, Ctx, M5]
    ): FlowWithExtendedContext[In, (Out1, Out2, Out3, Out4, Out5), Ctx, (M1, M2, M3, M4, M5)] = {
    val zipFlow = Flow.fromGraph(
      GraphDSL.createGraph(
        flow1.toGraph,
        flow2.toGraph,
        flow3.toGraph,
        flow4.toGraph,
        flow5.toGraph
      )((m1, m2, m3, m4, m5) => (m1, m2, m3, m4, m5)) { implicit builder =>
        import GraphDSL.Implicits._
        { case (f1, f2, f3, f4, f5) =>
          val bcast = builder.add(Broadcast.apply[(In, ExtendedContext[Ctx])](5, true))

          val zip1 = builder.add(Zip[(Out1, ExtendedContext[Ctx]), (Out2, ExtendedContext[Ctx])]())
          val zip2 = builder.add(
            Zip[
              (
                  (Out1, ExtendedContext[Ctx]),
                  (Out2, ExtendedContext[Ctx])
              ),
              (Out3, ExtendedContext[Ctx])
            ]()
          )
          val zip3 = builder.add(
            Zip[
              (
                  (
                      (Out1, ExtendedContext[Ctx]),
                      (Out2, ExtendedContext[Ctx])
                  ),
                  (Out3, ExtendedContext[Ctx])
              ),
              (Out4, ExtendedContext[Ctx])
            ]()
          )
          val zip4 = builder.add(
            Zip[
              (
                  (
                      (
                          (Out1, ExtendedContext[Ctx]),
                          (Out2, ExtendedContext[Ctx])
                      ),
                      (Out3, ExtendedContext[Ctx])
                  ),
                  (Out4, ExtendedContext[Ctx])
              ),
              (Out5, ExtendedContext[Ctx])
            ]()
          )

          bcast.outlets.zip(List(f1, f2, f3, f4, f5)).foreach { case (port, f) =>
            port ~> f
          }

          f1 ~> zip1.in0
          f2 ~> zip1.in1

          zip1.out ~> zip2.in0
          f3 ~> zip2.in1

          zip2.out ~> zip3.in0
          f4 ~> zip3.in1

          zip3.out ~> zip4.in0
          f5 ~> zip4.in1

          val adapter = builder.add {
            Flow[
              (
                  (
                      (
                          ((Out1, ExtendedContext[Ctx]), (Out2, ExtendedContext[Ctx])),
                          (Out3, ExtendedContext[Ctx])
                      ),
                      (Out4, ExtendedContext[Ctx])
                  ),
                  (Out5, ExtendedContext[Ctx])
              )
            ].map { case (((((o1, ctx), (o2, _)), (o3, _)), (o4, _)), (o5, _)) =>
              (o1, o2, o3, o4, o5) -> ctx
            }
          }

          zip4.out ~> adapter

          FlowShape(bcast.in, adapter.out)
        }
      }
    )

    zipFlow.asFlowWithExtendedContextUnsafe
  }

  /** Defers invoking the create function to create a future flow until there is downstream demand
    * and passing that downstream demand upstream triggers the first element.
    *
    * The materialized future value is completed with the materialized value of the created flow
    * when that has successfully been materialized.
    *
    * If the create function throws or returns a future that fails the stream is failed, in this
    * case the materialized future value is failed with a NeverMaterializedException.
    *
    * Note that asynchronous boundaries (and other operators) in the stream may do pre-fetching
    * which counter acts the laziness and can trigger the factory earlier than expected.
    */
  def lazyFlow[In, Out, Ctx, M](
      create: () => FlowWithExtendedContext[In, Out, Ctx, M]
    ) = {
    Flow.lazyFlow(() => create().toFlow).asFlowWithExtendedContextUnsafe
  }

  /** Object containing the implicit conversions needed to work with [[FlowWithExtendedContext]].
    *
    * Most of the implicit needed for working with [[FlowWithExtendedContext]] are automatically
    * discovered, however there are some situation where the compiler cannot find the required
    * implicit itself.
    *
    * In these situation you need to manually import this object as follows:
    *
    * ```
    * import FlowWithExtendedContext.sytanx._
    * ```
    *
    * This is usually only required to access the extension method `asFlowWithExtendedContextUnsafe`
    * on tupled flows of type `Flow[(In, ExtendedContext[Ctx]), (Out, ExtendedContext[Ctx]), _]`.
    */
  object syntax extends FlowWithExtendedContextSyntax
}

/** A [[FlowWithExtendedContext]] is much like a `FlowWithContext` but with the following additional
  * guarantees: any given input will be transformed into exactly one output.
  *
  * A [[FlowWithExtendedContext]] can be used to model effectively-once semantics.
  *
  * By using a persisted data source and associating each input value with a context, the flow
  * guarantees that once an output is produced, all computation related to the context has been
  * performed, thus the context can be marked as done. By restoring the source to the last completed
  * context and resuming computation we achieve at-least-once semantic.
  *
  * By employing a de-duplication strategy (based either on some property of the domain data or the
  * context itself), the guarantee can be elevated to effectively-once:
  *   - each input is processed at least once
  *   - already seen inputs are ignored
  *
  * A practical example would be to use a Kakfa source using the offset as a context, ordering the
  * flow to ensure in-order processing and committing the offsets as soon as an output is produced.
  */
final class FlowWithExtendedContext[-In, +Out, Ctx, +M] private[spekka] (
    delegate: Graph[FlowShape[(In, ExtendedContext[Ctx]), (Out, ExtendedContext[Ctx])], M]) {
  import FlowWithExtendedContext.syntax._

  /** Proxy for `Flow.withAttributes`
    */
  def withAttributes(attr: Attributes): FlowWithExtendedContext[In, Out, Ctx, M] = {
    new FlowWithExtendedContext(delegate.withAttributes(attr))
  }

  /** Proxy for `Flow.named`
    */
  def named(name: String): FlowWithExtendedContext[In, Out, Ctx, M] =
    new FlowWithExtendedContext(delegate.addAttributes(Attributes.name(name)))

  /** Proxy for `Flow.async`
    */
  def async: FlowWithExtendedContext[In, Out, Ctx, M] = new FlowWithExtendedContext(delegate.async)

  /** Proxy for `Flow.async`
    */
  def async(dispatcher: String, inputBufferSize: Int): FlowWithExtendedContext[In, Out, Ctx, M] =
    new FlowWithExtendedContext(delegate.async(dispatcher, inputBufferSize))

  /** Proxy for `Flow.addAttributes`
    */
  def addAttributes(attr: Attributes): FlowWithExtendedContext[In, Out, Ctx, M] =
    new FlowWithExtendedContext(delegate.addAttributes(attr))

  /** Converts this [[FlowWithExtendedContext]] into a tupled `Flow`
    */
  def toFlow: Flow[(In, ExtendedContext[Ctx]), (Out, ExtendedContext[Ctx]), M] =
    Flow.fromGraph(delegate)

  /** Converts this [[FlowWithExtendedContext]] into a `FlowWithContext`
    */
  def toFlowWithContext: FlowWithContext[In, ExtendedContext[Ctx], Out, ExtendedContext[Ctx], M] =
    FlowWithContext.fromTuples(Flow.fromGraph(delegate))

  /** Converts this [[FlowWithExtendedContext]] into a `Graph`
    */
  def toGraph: Graph[FlowShape[(In, ExtendedContext[Ctx]), (Out, ExtendedContext[Ctx])], M] =
    delegate

  /** Connect this [[FlowWithExtendedContext]] to another [[FlowWithExtendedContext]]
    * @param flow
    *   the other [[FlowWithExtendedContext]]
    */
  def via[Out2](
      flow: FlowWithExtendedContext[Out, Out2, Ctx, _]
    ): FlowWithExtendedContext[In, Out2, Ctx, M] =
    toFlow.via(flow.toFlow).asFlowWithExtendedContextUnsafe

  /** Connect this [[FlowWithExtendedContext]] to another [[FlowWithExtendedContext]] combining the
    * materialization values
    * @param flow
    *   the other [[FlowWithExtendedContext]]
    * @param combine
    *   the function used to combine materialization values
    */
  def viaMat[Out2, M2, M3](
      flow: FlowWithExtendedContext[Out, Out2, Ctx, M2]
    )(combine: (M, M2) => M3
    ): FlowWithExtendedContext[In, Out2, Ctx, M3] =
    toFlow.viaMat(flow.toFlow)(combine).asFlowWithExtendedContextUnsafe

  /** Equivalent of `Flow.map`
    *
    * @param f
    *   the function to apply to each element
    */
  def map[Out2](f: Out => Out2): FlowWithExtendedContext[In, Out2, Ctx, M] =
    toFlowWithContext.map(f).asFlowWithExtendedContextUnsafe

  /** Variant of `map` with read-only access to the context value
    *
    * @param f
    *   the function to apply to each element
    */
  def mapWithContext[Out2](f: (Out, Ctx) => Out2): FlowWithExtendedContext[In, Out2, Ctx, M] =
    toFlow.map { case (e, ctx) => f(e, ctx.innerContext) -> ctx }.asFlowWithExtendedContextUnsafe

  /** Allows to perform stateful transformation to the stream without affecting its cardinality.
    *
    * The semantics of the transformation function is the same as for `Flow.statefulMapConcat`,
    * however the produced outputs will not be concatenated.
    * @param f
    *   the stateful transformation function
    * @return
    *   transformed flow
    */
  def statefulMap[Out2](f: () => Out => Out2): FlowWithExtendedContext[In, Out2, Ctx, M] = {
    def wrappedF() = {
      val inst = f()

      in: (Out, ExtendedContext[Ctx]) => List(inst(in._1) -> in._2)
    }

    toFlow.statefulMapConcat(wrappedF).asFlowWithExtendedContextUnsafe
  }

  /** Variant of `statefulMap` with read-only access to the context value
    * @param f
    *   the stateful transformation function
    * @return
    *   transformed flow
    */
  def statefulMapWithContext[Out2](
      f: () => (Out, Ctx) => Out2
    ): FlowWithExtendedContext[In, Out2, Ctx, M] = {
    def wrappedF() = {
      val inst = f()

      in: (Out, ExtendedContext[Ctx]) => List(inst(in._1, in._2.innerContext) -> in._2)
    }

    toFlow.statefulMapConcat(wrappedF).asFlowWithExtendedContextUnsafe
  }

  /** Equivalent of `Flow.mapAsync`
    * @param parallelism
    *   the number of elements to process in parallel
    * @param f
    *   the function to apply to each element
    */
  def mapAsync[Out2](
      parallelism: Int
    )(f: Out => Future[Out2]
    ): FlowWithExtendedContext[In, Out2, Ctx, M] =
    toFlowWithContext.mapAsync(parallelism)(f).asFlowWithExtendedContextUnsafe

  /** Variant of `mapAsync` with read-only access to the context value
    * @param parallelism
    *   the number of elements to process in parallel
    * @param f
    *   the function to apply to each element
    */
  def mapAsyncWithContext[Out2](
      parallelism: Int
    )(f: (Out, Ctx) => Future[Out2]
    )(implicit ec: ExecutionContext
    ): FlowWithExtendedContext[In, Out2, Ctx, M] =
    toFlow
      .mapAsync(parallelism) { case (e, ctx) => f(e, ctx.innerContext).map(_ -> ctx) }
      .asFlowWithExtendedContextUnsafe

  /** Equivalent of `Flow.mapMaterializedValue`
    *
    * @param f
    *   the materialization value transformation function
    */
  def mapMaterializedValue[M2](f: M => M2): FlowWithExtendedContext[In, Out, Ctx, M2] =
    toFlowWithContext.mapMaterializedValue(f).asFlowWithExtendedContextUnsafe

  /** Make the output of this flow be emitted in the same order as the corresponding input.
    *
    * The resulting flow will allow up to `bufferSize` elements to be processed before
    * back-pressuring. Back-pressure is relieved when the next in-order output is produced.
    *
    * @param bufferSize
    *   the size of the unordered buffer
    */
  def ordered(bufferSize: Int = 256): FlowWithExtendedContext[In, Out, Ctx, M] =
    Ordered(this, bufferSize)
}
