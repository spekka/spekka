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

import akka.NotUsed
import akka.stream.Attributes
import akka.stream.FlowShape
import akka.stream.Graph
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.FlowWithContext
import akka.stream.scaladsl.Keep
import spekka.stream.ExtendedContext
import spekka.stream.scaladsl.internal._

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

    def asFlowWithPreservedContextUnsafe: FlowWithExtendedContext[In, Out, Ctx, M] =
      new FlowWithExtendedContext(graph)
  }

  implicit class FlowWithExtendedContextListOps[In, OutE, Out <: immutable.Iterable[OutE], Ctx, M](
      backingFlow: FlowWithExtendedContext[In, Out, Ctx, M]) {
    def viaMultiplexed[Out2](
        flow: FlowWithExtendedContext[OutE, Out2, Ctx, _]
      ): FlowWithExtendedContext[In, immutable.Iterable[Out2], Ctx, M] =
      backingFlow.via(Multiplexed(flow))
  }
}

object FlowWithExtendedContext extends FlowWithExtendedContextSyntax {
  def apply[In, Ctx]: FlowWithExtendedContext[In, In, Ctx, NotUsed] = {
    val backingFlow = FlowWithContext[In, ExtendedContext[Ctx]]
    new FlowWithExtendedContext[In, In, Ctx, NotUsed](backingFlow)
  }

  def fromGraphUnsafe[In, Out, Ctx, M](
      graph: Graph[FlowShape[(In, ExtendedContext[Ctx]), (Out, ExtendedContext[Ctx])], M]
    ): FlowWithExtendedContext[In, Out, Ctx, M] = new FlowWithExtendedContext(graph)

  object syntax extends FlowWithExtendedContextSyntax
}

final class FlowWithExtendedContext[-In, +Out, Ctx, +M] private[scaladsl] (
    delegate: Graph[FlowShape[(In, ExtendedContext[Ctx]), (Out, ExtendedContext[Ctx])], M]) {
  import FlowWithExtendedContext.syntax._

  def withAttributes(attr: Attributes): FlowWithExtendedContext[In, Out, Ctx, M] = {
    new FlowWithExtendedContext(delegate.withAttributes(attr))
  }

  def named(name: String): FlowWithExtendedContext[In, Out, Ctx, M] =
    new FlowWithExtendedContext(delegate.addAttributes(Attributes.name(name)))

  def async: FlowWithExtendedContext[In, Out, Ctx, M] = new FlowWithExtendedContext(delegate.async)

  def async(dispatcher: String, inputBufferSize: Int): FlowWithExtendedContext[In, Out, Ctx, M] =
    new FlowWithExtendedContext(delegate.async(dispatcher, inputBufferSize))

  def addAttributes(attr: Attributes): FlowWithExtendedContext[In, Out, Ctx, M] =
    new FlowWithExtendedContext(delegate.addAttributes(attr))

  def toFlow: Flow[(In, ExtendedContext[Ctx]), (Out, ExtendedContext[Ctx]), M] =
    Flow.fromGraph(delegate)

  def toFlowWithContext: FlowWithContext[In, ExtendedContext[Ctx], Out, ExtendedContext[Ctx], M] =
    FlowWithContext.fromTuples(Flow.fromGraph(delegate))

  def toGraph: Graph[FlowShape[(In, ExtendedContext[Ctx]), (Out, ExtendedContext[Ctx])], M] =
    delegate

  def via[Out2](
      flow: FlowWithExtendedContext[Out, Out2, Ctx, _]
    ): FlowWithExtendedContext[In, Out2, Ctx, M] =
    toFlow.via(flow.toFlow).asFlowWithPreservedContextUnsafe

  def viaMat[Out2, M2, M3](
      flow: FlowWithExtendedContext[Out, Out2, Ctx, M2]
    )(combine: (M, M2) => M3
    ): FlowWithExtendedContext[In, Out2, Ctx, M3] =
    toFlow.viaMat(flow.toFlow)(combine).asFlowWithPreservedContextUnsafe

  def map[Out2](f: Out => Out2): FlowWithExtendedContext[In, Out2, Ctx, M] =
    toFlowWithContext.map(f).asFlowWithPreservedContextUnsafe

  def mapAsync[Out2](
      parallelism: Int
    )(f: Out => Future[Out2]
    ): FlowWithExtendedContext[In, Out2, Ctx, M] =
    toFlowWithContext.mapAsync(parallelism)(f).asFlowWithPreservedContextUnsafe

  private[stream] def mapContextUnsafe[Ctx2](
      f: ExtendedContext[Ctx] => ExtendedContext[Ctx]
    ): FlowWithExtendedContext[In, Out, Ctx, M] =
    toFlowWithContext.mapContext(f).asFlowWithPreservedContextUnsafe

  def mapMaterializedValue[M2](f: M => M2): FlowWithExtendedContext[In, Out, Ctx, M2] =
    toFlowWithContext.mapMaterializedValue(f).asFlowWithPreservedContextUnsafe

  def ordered: FlowWithExtendedContext[In, Out, Ctx, M] = Ordered(this)
}
