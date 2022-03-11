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
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.FlowWithContext
import akka.stream.scaladsl.Keep
import spekka.context.internal._

import scala.collection.immutable
import scala.concurrent.Future

trait FlowWithExtendedContextSyntax {
  implicit def toWrappingGraph[In, Out, Ctx, M](
      flow: FlowWithExtendedContext[In, Out, Ctx, M]
    ): Graph[FlowShape[(In, Ctx), (Out, Ctx)], M] = {
    Flow[(In, Ctx)]
      .map { case (out, ctx) => out -> ExtendedContext(ctx) }
      .viaMat(flow.toGraph)(Keep.right)
      .map { case (out, ectx) => out -> ectx.innerContext }
  }

  implicit def toGraph[In, Out, Ctx, M](
      flow: FlowWithExtendedContext[In, Out, Ctx, M]
    ): Graph[FlowShape[(In, ExtendedContext[Ctx]), (Out, ExtendedContext[Ctx])], M] = {
    flow.toGraph
  }

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

  implicit class FlowWithExtendedContextListOps[In, OutE, Out <: immutable.Iterable[OutE], Ctx, M](
      backingFlow: FlowWithExtendedContext[In, Out, Ctx, M]) {

    /** Transform this flow by piping each element of the output through the given flow, recombining
      * the results back into a collection.
      *
      * @param flow
      *   the flow used to transform each output elements
      */
    def viaMultiplexed[Out2](
        flow: FlowWithExtendedContext[OutE, Out2, Ctx, _]
      ): FlowWithExtendedContext[In, immutable.Iterable[Out2], Ctx, M] =
      backingFlow.via(Multiplexed(flow))
  }
}

object FlowWithExtendedContext extends FlowWithExtendedContextSyntax {

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

  private[spekka] def mapContextUnsafe[Ctx2](
      f: ExtendedContext[Ctx] => ExtendedContext[Ctx]
    ): FlowWithExtendedContext[In, Out, Ctx, M] =
    toFlowWithContext.mapContext(f).asFlowWithExtendedContextUnsafe

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
