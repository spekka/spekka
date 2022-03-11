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

import akka.stream.Attributes
import akka.stream.FanInShape2
import akka.stream.FanOutShape2
import akka.stream.FlowShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.GraphDSL
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
import spekka.context.ExtendedContext
import spekka.context.FlowWithExtendedContext

import scala.annotation.tailrec
import scala.collection.mutable

private[spekka] object Ordered {

  class PreStage[In, Ctx]
      extends GraphStage[
        FanInShape2[(In, ExtendedContext[Ctx]), Int, (In, ExtendedContext[Ctx])]
      ] {

    val dataIn: Inlet[(In, ExtendedContext[Ctx])] = Inlet(s"Ordered.PreStage.dataIn")
    val availabilityIn: Inlet[Int] = Inlet("Ordered.PreStage.availabilityIn")
    val out: Outlet[(In, ExtendedContext[Ctx])] = Outlet("Ordered.PreStage.out")

    override def shape: FanInShape2[(In, ExtendedContext[Ctx]), Int, (In, ExtendedContext[Ctx])] =
      new FanInShape2(dataIn, availabilityIn, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) {
        var seqNr: Long = 0
        var availabilityCount: Int = 0

        override def preStart(): Unit = pull(availabilityIn)

        setHandler(
          out,
          new OutHandler {
            override def onPull(): Unit = if (availabilityCount > 0) { pull(dataIn) }
          }
        )

        setHandler(
          dataIn,
          new InHandler {

            override def onPush(): Unit = {
              val (data, ctx) = grab(dataIn)
              push(out, data -> ctx.push(SequenceNumberContext(seqNr)))
              seqNr += 1
              availabilityCount -= 1
            }
          }
        )

        setHandler(
          availabilityIn,
          new InHandler {
            override def onPush(): Unit = {
              val c = grab(availabilityIn)
              availabilityCount += c
              if (!hasBeenPulled(dataIn) && isAvailable(out)) pull(dataIn)
              pull(availabilityIn)
            }
          }
        )
      }
  }

  class PostStage[Out, Ctx](bufferSize: Int)
      extends GraphStage[
        FanOutShape2[(Out, ExtendedContext[Ctx]), (Out, ExtendedContext[Ctx]), Int]
      ] {
    val in: Inlet[(Out, ExtendedContext[Ctx])] = Inlet("Ordered.PostStage.in")
    val dataOut: Outlet[(Out, ExtendedContext[Ctx])] = Outlet("Ordered.PostStage.dataOut")
    val availabilityOut: Outlet[Int] = Outlet("Ordered.PostStage.availabilityOut")

    override def shape
        : FanOutShape2[(Out, ExtendedContext[Ctx]), (Out, ExtendedContext[Ctx]), Int] =
      new FanOutShape2(in, dataOut, availabilityOut)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) {
        var nextSeqNr: Long = 0

        val bufferQueue: mutable.PriorityQueue[(Out, ExtendedContext[Ctx], Long)] =
          mutable.PriorityQueue[(Out, ExtendedContext[Ctx], Long)]()(
            Ordering.by { case (_, _, seqNr) =>
              -seqNr
            }
          )

        var initialized = false

        def drainBuffer(): List[(Out, ExtendedContext[Ctx])] =
          if (bufferQueue.nonEmpty) {
            @tailrec
            def go(acc: List[(Out, ExtendedContext[Ctx])]): List[(Out, ExtendedContext[Ctx])] =
              bufferQueue.headOption match {
                case Some((msg, ctx, seqNr)) if seqNr == nextSeqNr =>
                  bufferQueue.dequeue()
                  nextSeqNr += 1
                  go((msg -> ctx) :: acc)

                case _ => acc
              }

            go(Nil).reverse
          } else Nil

        setHandler(
          in,
          new InHandler {
            override def onUpstreamFinish(): Unit =
              if (bufferQueue.nonEmpty) {
                failStage(
                  new IllegalStateException(
                    s"Upstream completed while still waiting for some element (i.e. someone filtered) bufferSize: ${bufferQueue.size}, expected Sequence Number: $nextSeqNr"
                  )
                )
              } else { super.onUpstreamFinish() }

            override def onPush(): Unit = {
              val (msg, ctx) = grab(in)

              val (ctx1, seqCtx) = ctx.popOrElse[SequenceNumberContext](
                throw new IllegalStateException(
                  s"Received message with a non head context of ${classOf[SequenceNumberContext].getName()}"
                )
              )

              if (seqCtx.seqNr < nextSeqNr && seqCtx.seqNr < nextSeqNr + bufferSize) {
                failStage(
                  new IllegalStateException(
                    s"Received old sequence number. Expected minimum [$nextSeqNr]"
                  )
                )
              } else if (seqCtx.seqNr == nextSeqNr) {
                nextSeqNr += 1
                val outs = drainBuffer()
                val allOuts = (msg -> ctx1) :: outs
                if (!isClosed(availabilityOut)) { emit(availabilityOut, allOuts.size) }
                emitMultiple(dataOut, allOuts)
              } else {
                bufferQueue.enqueue((msg, ctx1, seqCtx.seqNr))
                if (bufferQueue.size >= bufferSize)
                  failStage(
                    new IllegalStateException(
                      s"Buffer size exceeded! This should not happen! Size = ${bufferQueue.size}"
                    )
                  )
                pull(in)
              }
            }
          }
        )

        setHandler(
          availabilityOut,
          new OutHandler {
            override def onPull(): Unit =
              if (!initialized) {
                push(availabilityOut, bufferSize)
                initialized = true
              }

            /* Downstream is actually the pre stage, hence when it finishes it only means that its upstream completed.
             * We still need to process all the in-flight messages before completing this stage.
             * So we need to overwrite the default behavior and do nothing
             */
            override def onDownstreamFinish(t: Throwable): Unit = {}
          }
        )

        setHandler(
          dataOut,
          new OutHandler {
            override def onPull() = {
              pull(in)
            }

            override def onDownstreamFinish(cause: Throwable) = {
              failStage(cause)
            }
          }
        )
      }
  }

  def apply[In, Out, Ctx, M](
      flow: FlowWithExtendedContext[In, Out, Ctx, M],
      bufferSize: Int
    ): FlowWithExtendedContext[In, Out, Ctx, M] = {
    import FlowWithExtendedContext.syntax._
    Flow
      .fromGraph(GraphDSL.createGraph(flow.toFlow) { implicit builder =>
        import GraphDSL.Implicits._
        _flow =>
          val preStage = builder.add(new PreStage[In, Ctx])
          val postStage = builder.add(new PostStage[Out, Ctx](bufferSize))

          preStage.out ~> _flow ~> postStage.in
          preStage.in1 <~ postStage.out1

          FlowShape(preStage.in0, postStage.out0)
      })
      .asFlowWithExtendedContextUnsafe
  }
}
