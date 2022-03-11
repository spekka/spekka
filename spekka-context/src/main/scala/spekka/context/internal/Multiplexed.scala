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

package spekka.context.internal

import akka.NotUsed
import akka.stream.Attributes
import akka.stream.FlowShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Keep
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
import spekka.context.ExtendedContext
import spekka.context.FlowWithExtendedContext
import spekka.context.Partition

import scala.collection.immutable
import scala.collection.mutable

private[spekka] object Multiplexed {
  class UnorderedMultiplexStage[In, Ctx]
      extends GraphStage[
        FlowShape[(In, ExtendedContext[Ctx]), (immutable.Iterable[In], ExtendedContext[Ctx])]
      ] {

    val in = Inlet[(In, ExtendedContext[Ctx])]("UnorderedMultiplexStage.in")
    val out =
      Outlet[(immutable.Iterable[In], ExtendedContext[Ctx])]("UnorderedMultiplexStage.out")

    override def shape
        : FlowShape[(In, ExtendedContext[Ctx]), (immutable.Iterable[In], ExtendedContext[Ctx])] =
      FlowShape(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) {
        val contextBufferMap: mutable.Map[Int, mutable.ListBuffer[In]] = mutable.Map()
        val outputBuffer: mutable.ListBuffer[(immutable.Iterable[In], ExtendedContext[Ctx])] =
          mutable.ListBuffer()

        def pushOrBuffer(data: immutable.Iterable[In], ctx: ExtendedContext[Ctx]): Unit = {
          if (isAvailable(out)) push(out, data -> ctx)
          else outputBuffer += data -> ctx
        }

        setHandler(
          in,
          new InHandler {
            override def onPush(): Unit = {
              val (value, ctx) = grab(in)
              val (outCtx, maybeMultiplexedContext) = ctx.pop[MultiplexedContext]

              maybeMultiplexedContext match {
                case Some(mCtx) if mCtx.n == 0 =>
                  pushOrBuffer(Nil, outCtx)

                case Some(mCtx) if mCtx.n == 1 =>
                  pushOrBuffer(List(value), outCtx)

                case Some(mCtx) =>
                  val ctxBuffer = contextBufferMap.getOrElseUpdate(mCtx.hash, mutable.ListBuffer())
                  if (ctxBuffer.size + 1 == mCtx.n) {
                    pushOrBuffer(value :: ctxBuffer.toList, outCtx)
                  } else {
                    ctxBuffer += value
                    pull(in)
                  }

                case None =>
                  failStage(new IllegalStateException("MultiplexedContext not found!"))
              }
            }

            override def onUpstreamFinish() = {
              if (outputBuffer.isEmpty) super.onUpstreamFinish()
            }
          }
        )

        setHandler(
          out,
          new OutHandler {
            override def onPull(): Unit = {
              if (outputBuffer.nonEmpty) push(out, outputBuffer.remove(0))
              else if (!isClosed(in)) pull(in)
              else completeStage()
            }
          }
        )
      }
  }

  private[spekka] class OrderedMultiplexStage[In, Ctx]
      extends GraphStage[
        FlowShape[(In, ExtendedContext[Ctx]), (immutable.Iterable[In], ExtendedContext[Ctx])]
      ] {
    val in = Inlet[(In, ExtendedContext[Ctx])]("OrderedMultiplexStage.in")
    val out =
      Outlet[(immutable.Iterable[In], ExtendedContext[Ctx])]("OrderedMultiplexStage.out")

    override def shape
        : FlowShape[(In, ExtendedContext[Ctx]), (immutable.Iterable[In], ExtendedContext[Ctx])] =
      FlowShape(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) {
        var currCtxHash: Option[Int] = None
        val resultBuff = mutable.ListBuffer[In]()

        setHandler(
          in,
          new InHandler {
            override def onPush(): Unit = {
              val (value, ctx) = grab(in)
              val (outCtx, mCtx) = ctx.popOrElse[MultiplexedContext](
                throw new IllegalStateException("MultiplexedContext not found!")
              )

              if (mCtx.n == 0) {
                if (currCtxHash.nonEmpty)
                  throw new IllegalStateException(
                    "Received context differs from current multiplexed one. Upstream flow has reordered/filtered!"
                  )
                emit(out, Nil -> outCtx)
              } else if (mCtx.n == 1) {
                if (currCtxHash.nonEmpty)
                  throw new IllegalStateException(
                    "Received context differs from current multiplexed one. Upstream flow has reordered/filtered!"
                  )

                emit(out, List(value) -> outCtx)
              } else {
                currCtxHash match {
                  case Some(hash) =>
                    if (hash != mCtx.hash)
                      throw new IllegalStateException(
                        "Received context differs from current multiplexed one. Upstream flow has reordered/filtered!"
                      )

                  case None => currCtxHash = Some(mCtx.hash)
                }

                resultBuff += value
                if (mCtx.n == resultBuff.size) {
                  val results = resultBuff.toList
                  resultBuff.clear()
                  currCtxHash = None
                  emit(out, results -> outCtx)
                } else {
                  pull(in)
                }
              }
            }

            override def onUpstreamFinish(): Unit =
              if (resultBuff.nonEmpty) {
                failStage(
                  new IllegalStateException(
                    s"Upstream finished but multiplexed context buffer non empty! Upstream flow has reordered/filtered!"
                  )
                )
              } else super.onUpstreamFinish()

            override def onUpstreamFailure(ex: Throwable): Unit = super.onUpstreamFailure(ex)
          }
        )

        setHandler(
          out,
          new OutHandler {
            override def onPull(): Unit = pull(in)
          }
        )
      }
  }

  /** Creates a flow which route each element of the input to the `elementFlow` and produce a single
    * result containing all the outputs.
    *
    * @param elementFlow
    *   The flow to use for single elements of the input iterable
    * @return
    *   Flow accepting and iterable of inputs and producing an iterable of outputs
    */
  def apply[In, Out, Ctx, M](
      elementFlow: FlowWithExtendedContext[In, Out, Ctx, M]
    ): FlowWithExtendedContext[immutable.Iterable[In], immutable.Iterable[Out], Ctx, M] = {
    sealed trait PartitionKey
    object MultiplexFlow extends PartitionKey
    object PassthroughContext extends PartitionKey

    import FlowWithExtendedContext.syntax._

    val multiplexedFlow = Flow[(immutable.Iterable[In], ExtendedContext[Ctx])]
      .mapConcat { case (ins, ctx) =>
        val n = ins.size
        val outCtx = ctx.push(MultiplexedContext(ctx.hashCode(), n))
        ins.iterator.map(_ -> outCtx).toList
      }
      .asFlowWithExtendedContextUnsafe
      .viaMat(elementFlow)(Keep.right)
      .via(
        Flow
          .fromGraph(new OrderedMultiplexStage[Out, Ctx])
          .asFlowWithExtendedContextUnsafe
      )

    val passthroughFlow
        : FlowWithExtendedContext[immutable.Iterable[In], immutable.Iterable[Out], Ctx, NotUsed] =
      FlowWithExtendedContext[immutable.Iterable[In], Ctx].map(_ => Nil)

    def partitioner(in: Iterable[In]): PartitionKey =
      if (in.nonEmpty) MultiplexFlow
      else PassthroughContext

    Partition
      .staticMat[immutable.Iterable[In], immutable.Iterable[Out], Ctx, PartitionKey, M, Any, Any](
        (in, _) => partitioner(in),
        FlowWithExtendedContext[immutable.Iterable[In], Ctx].map(_ =>
          throw new IllegalStateException("MultiplexedFlow inconsistency!")
        )
      )(
        MultiplexFlow -> multiplexedFlow,
        PassthroughContext -> passthroughFlow
      )
      .mapMaterializedValue(_._1)
  }
}
