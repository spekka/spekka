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

import akka.Done
import akka.stream.Attributes
import akka.stream.FlowShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.scaladsl.Keep
import akka.stream.stage.AsyncCallback
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.GraphStageWithMaterializedValue
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
import akka.stream.stage.TimerGraphStageLogic
import spekka.context.ExtendedContext
import spekka.context.FlowWithExtendedContext
import spekka.context.PartitionDynamic
import spekka.context.PartitionDynamic.CompletionCriteria
import spekka.context.SubStream

import scala.collection.mutable
import scala.concurrent.Promise
import scala.util.Failure
import scala.util.Success
import scala.util.Try

private[spekka] object PartitionDynamicInternal {
  sealed trait Partitioner[In, Out, Ctx, K, M] {
    def flowForKey(key: K): FlowWithExtendedContext[In, Out, Ctx, M]
    def flowForContextPassthrough: FlowWithExtendedContext[In, Out, Ctx, _]
  }
  object Partitioner {
    case class SinglePartitioner[In, Out, Ctx, K, M](
        keyF: (In, Ctx) => K,
        flowF: K => FlowWithExtendedContext[In, Out, Ctx, M],
        passthroughFlow: FlowWithExtendedContext[In, Out, Ctx, _]
      ) extends Partitioner[In, Out, Ctx, K, M] {
      override def flowForKey(key: K): FlowWithExtendedContext[In, Out, Ctx, M] = flowF(key)
      override def flowForContextPassthrough: FlowWithExtendedContext[In, Out, Ctx, _] =
        passthroughFlow
    }

    case class MultiPartitioner[In, Out, Ctx, K, M](
        keyF: (In, Ctx, Set[K]) => Set[K],
        flowF: K => FlowWithExtendedContext[In, Out, Ctx, M],
        passthroughFlow: FlowWithExtendedContext[In, Out, Ctx, _]
      ) extends Partitioner[In, Out, Ctx, K, M] {
      override def flowForKey(key: K): FlowWithExtendedContext[In, Out, Ctx, M] = flowF(key)
      override def flowForContextPassthrough: FlowWithExtendedContext[In, Out, Ctx, _] =
        passthroughFlow
    }
  }

  class PartitionStage[K, In, Out, Ctx, M](
      partitioner: Partitioner[In, Out, Ctx, K, M],
      completionCriteria: CompletionCriteria[In, Out, Ctx],
      partitionAutoCreation: Boolean,
      bufferSize: Int,
      initialKeys: Set[K]
    ) extends GraphStageWithMaterializedValue[
        FlowShape[(In, ExtendedContext[Ctx]), (Out, ExtendedContext[Ctx])],
        PartitionDynamic.Control[K, M]
      ] {

    val in: Inlet[(In, ExtendedContext[Ctx])] = Inlet("ParitionDynamic.PartitionStage.in")
    val out: Outlet[(Out, ExtendedContext[Ctx])] = Outlet("PartitionDynamic.PartitionStage.out")

    override def shape: FlowShape[(In, ExtendedContext[Ctx]), (Out, ExtendedContext[Ctx])] =
      FlowShape(in, out)

    case class HandlerDescr(
        out: SubStream.SubOutlet[(In, ExtendedContext[Ctx])],
        in: SubStream.SubInlet[(Out, ExtendedContext[Ctx])],
        materializedValue: M
      )

    class PartitionLogic(shape: Shape) extends TimerGraphStageLogic(shape) with SubStream {
      var liveKeys: Set[K] = Set()
      var globalSeqNr: Long = 0

      val handlersSubOutletMap: mutable.Map[Option[K], HandlerDescr] =
        mutable.Map()

      val handlersSeqNrMap: mutable.Map[Option[K], Long] = mutable.Map()
      val handlersCompletionRequestMap: mutable.Map[Option[K], mutable.Set[Long]] = mutable.Map()

      var failure: Option[Throwable] = None
      var inflight: Int = 0
      val outputBuffer: mutable.ListBuffer[(Out, ExtendedContext[Ctx])] = mutable.ListBuffer()

      val getKeysCallback: AsyncCallback[Promise[Set[K]]] = getAsyncCallback { p =>
        p.success(liveKeys)
      }

      val spawnKeyCallback: AsyncCallback[(K, Promise[M])] = getAsyncCallback {
        case (key, promise) =>
          val hd = handlersSubOutletMap.getOrElse(Some(key), spawnHandlerForKey(Some(key)))
          promise.success(hd.materializedValue)
      }

      val completeKeyCallback: AsyncCallback[(K, Promise[Done])] = getAsyncCallback {
        case (key, promise) =>
          completeHandlerForKey(Some(key))
          promise.success(Done)
      }

      val recreateKeyCallback: AsyncCallback[(K, Promise[(Option[M], M)])] = getAsyncCallback {
        case (key, promise) =>
          val preM = handlersSubOutletMap.get(Some(key)).map(_.materializedValue)
          completeHandlerForKey(Some(key))
          val postM = spawnHandlerForKey(Some(key)).materializedValue
          promise.success(preM -> postM)
      }

      val withMaterializedValueCallback: AsyncCallback[(K, M => Any, Promise[Option[Any]])] =
        getAsyncCallback { case (key, f, promise) =>
          handlersSubOutletMap.get(Some(key)) match {
            case Some(hd) =>
              Try(f(hd.materializedValue)) match {
                case Success(v) => promise.success(Some(v))
                case Failure(ex) => promise.failure(ex)
              }
            case None =>
              promise.success(None)
          }
        }

      override def preStart(): Unit = {
        initialKeys.foreach(k => spawnHandlerForKey(Some(k)))
      }

      def handlerOutLogic: SubStream.SubOutHandler[(In, ExtendedContext[Ctx])] = {
        new SubStream.SubOutHandler[(In, ExtendedContext[Ctx])] {
          override def initialized(
              subOutlet: SubStream.SubOutlet[(In, ExtendedContext[Ctx])]
            ): Unit = {}
          override def onPull(
              subOutlet: SubStream.SubOutlet[(In, ExtendedContext[Ctx])]
            ): Unit = {
            pullIfNeeded()
          }

          override def onDownstreamFinish(
              subOutlet: SubStream.SubOutlet[(In, ExtendedContext[Ctx])],
              cause: Throwable
            ): Unit = {
            // Failures are handled by the sink
          }
        }
      }

      def handlerInLogic(key: Option[K]): SubStream.SubInHandler[(Out, ExtendedContext[Ctx])] = {
        new SubStream.SubInHandler[(Out, ExtendedContext[Ctx])] {
          var lastSubSeqNr: Long = -1

          override def initialized(
              subInlet: SubStream.SubInlet[(Out, ExtendedContext[Ctx])]
            ): Unit = {
            subInlet.pull()
          }

          override def onPush(
              subInlet: SubStream.SubInlet[(Out, ExtendedContext[Ctx])],
              element: (Out, ExtendedContext[Ctx])
            ): Unit = {
            inflight -= 1
            val (data, ctx) = element

            val (newCtx, maybeOutSeqNr) = ctx.pop[SequenceNumberContext]
            val outSeqNr = maybeOutSeqNr.getOrElse(
              throw new IllegalStateException(
                "Inconsistency in PartitionDynamic: subflow context doesn't have sequence number"
              )
            )

            if (lastSubSeqNr < outSeqNr.seqNr) lastSubSeqNr = outSeqNr.seqNr

            if (
              completionCriteria
                .shouldCompleteOnOutput(data, ctx) && handlersSeqNrMap(key) == outSeqNr.seqNr
            ) {
              completeHandlerForKey(key)
            }

            pushOrBuffer(data -> newCtx)
            subInlet.pull()
            pullIfNeeded()
          }

          override def onUpstreamFinish(
              subInlet: SubStream.SubInlet[(Out, ExtendedContext[Ctx])]
            ): Unit = {
            handlersCompletionRequestMap.get(key) match {
              case Some(pendingCompletionSeqNrs) =>
                if (!pendingCompletionSeqNrs.remove(lastSubSeqNr)) {
                  handlerFailed(
                    key,
                    new IllegalStateException(
                      "Subflow completed with unknown sequence number"
                    )
                  )
                } else if (pendingCompletionSeqNrs.isEmpty) {
                  handlersCompletionRequestMap -= key
                  if (!handlersSubOutletMap.contains(key)) {
                    handlersSeqNrMap -= key
                    stopIfNeeded()
                  }
                }
              case None =>
                handlerFailed(
                  key,
                  new IllegalStateException("Subflow completed with no pending requests")
                )
            }
          }

          override def onUpstreamFailure(
              subInlet: SubStream.SubInlet[(Out, ExtendedContext[Ctx])],
              cause: Throwable
            ): Unit = {
            handlerFailed(
              key,
              new IllegalStateException(s"Partition handler for key $key failed", cause)
            )
          }
        }
      }

      // == Handlers management
      def startHandlerForKey(
          key: Option[K]
        ): HandlerDescr = {
        completionCriteria.completeOnIdle.foreach { timeout =>
          scheduleOnce(key -> 1L, timeout)
        }

        val (subOut, src) = getSubSource(1, handlerOutLogic)
        val (subIn, sink) = getSubSink(1, handlerInLogic(key))

        key match {
          case Some(k) =>
            val matVal = src
              .viaMat(partitioner.flowForKey(k))(Keep.right)
              .toMat(sink)(Keep.left)
              .run()(subFusingMaterializer)
            HandlerDescr(subOut, subIn, matVal)
          case None =>
            src.via(partitioner.flowForContextPassthrough).runWith(sink)(subFusingMaterializer)
            HandlerDescr(subOut, subIn, null.asInstanceOf[M])
        }

      }

      def spawnHandlerForKey(key: Option[K]): HandlerDescr = {
        key.foreach { k => liveKeys = liveKeys + k }
        val hd = startHandlerForKey(key)
        handlersSubOutletMap += key -> hd
        hd
      }

      def getHandlerForKey(
          key: Option[K]
        ): (Option[K], SubStream.SubOutlet[(In, ExtendedContext[Ctx])]) = {
        if (partitionAutoCreation || key.isEmpty)
          key -> handlersSubOutletMap.getOrElse(key, spawnHandlerForKey(key)).out
        else {
          handlersSubOutletMap.get(key).map(s => key -> s.out).getOrElse(getHandlerForKey(None))
        }
      }

      def handleInputForKey(key: Option[K], data: In, ctx: ExtendedContext[Ctx]): Unit = {
        val (actualKey, subOutlet) = getHandlerForKey(key)
        val seqNr = nextSequenceNumberForKey(actualKey)
        val newCtx = ctx.push(SequenceNumberContext(seqNr))

        inflight += 1

        subOutlet.pushOrBuffer(data -> newCtx)
        if (completionCriteria.shouldCompleteOnInput(data, newCtx)) {
          completeHandlerForKey(actualKey)
        }

        pullIfNeeded()
      }

      def completeHandlerForKey(key: Option[K]): Unit = {
        key.foreach { k => liveKeys = liveKeys - k }
        handlersSubOutletMap.remove(key).foreach { subOutlet =>
          subOutlet.out.complete()
          handlersCompletionRequestMap.getOrElseUpdate(key, mutable.Set()) += handlersSeqNrMap(
            key
          )
        }
      }

      def nextSequenceNumberForKey(key: Option[K]): Long = {
        val seqNr = handlersSeqNrMap.getOrElse(key, 0L) + 1
        handlersSeqNrMap += key -> seqNr
        seqNr
      }

      def failHandlersThenFailStage(cause: Throwable): Unit = {
        if (failure.isEmpty) {
          failure = Some(cause)
          cancel(in, cause)

          handlersSubOutletMap.valuesIterator.foreach { handlerPorts =>
            handlerPorts.out.fail(cause)
          }
        }

        stopIfNeeded()
      }

      def handlerFailed(key: Option[K], cause: Throwable): Unit = {
        handlersSubOutletMap -= key
        handlersCompletionRequestMap -= key
        handlersSeqNrMap -= key
        failHandlersThenFailStage(cause)
      }

      // Stage management
      def stopIfNeeded(): Unit = {
        if (isClosed(in) && handlersSeqNrMap.isEmpty && outputBuffer.isEmpty) {
          failure match {
            case Some(cause) =>
              failStage(cause)
            case None =>
              completeStage()
          }
        }
      }

      def pushOrBuffer(msg: (Out, ExtendedContext[Ctx])): Unit = {
        if (isAvailable(out)) push(out, msg)
        else outputBuffer.append(msg)
      }

      def pullIfNeeded(): Unit = {
        if (!hasBeenPulled(in) && (inflight + outputBuffer.size < bufferSize)) {
          tryPull(in)
        }
      }

      override protected def onTimer(timerKey: Any): Unit =
        timerKey match {
          case (key: Option[K] @unchecked, seqNr: Long) =>
            handlersSeqNrMap.get(key).foreach { currentSeqNr =>
              if (seqNr == currentSeqNr) completeHandlerForKey(key)
              else scheduleOnce(key -> currentSeqNr, completionCriteria.completeOnIdle.get)
            }
          case _ => throw new IllegalArgumentException("Unknown timer")
        }

      abstract class BaseInHandler extends InHandler {
        override def onUpstreamFinish() = {
          handlersSubOutletMap.keysIterator.foreach(completeHandlerForKey)
          stopIfNeeded()
        }

        override def onUpstreamFailure(ex: Throwable): Unit = {
          failHandlersThenFailStage(ex)
        }
      }

      class SingleInHandler(singlePartitioner: Partitioner.SinglePartitioner[In, Out, Ctx, K, M])
          extends BaseInHandler {
        override def onPush(): Unit = {
          val (data, ctx) = grab(in)

          val key = singlePartitioner.keyF(data, ctx.innerContext)
          handleInputForKey(Some(key), data, ctx)
        }
      }

      class MultiInHandler(multiPartitioner: Partitioner.MultiPartitioner[In, Out, Ctx, K, M])
          extends BaseInHandler {
        override def onPush(): Unit = {
          val (data, ctx) = grab(in)
          globalSeqNr += 1
          val keys = multiPartitioner.keyF(data, ctx.innerContext, liveKeys)
          val newCtx = ctx.push(
            MultiplexedContext(
              globalSeqNr,
              Math.max(keys.size, 1)
            )
          )
          if (keys.nonEmpty) {
            for (key <- keys) handleInputForKey(Some(key), data, newCtx)
          } else {
            handleInputForKey(None, data, newCtx)
          }
        }
      }

      partitioner match {
        case single: Partitioner.SinglePartitioner[In, Out, Ctx, K, M] =>
          setHandler(in, new SingleInHandler(single))
        case multi: Partitioner.MultiPartitioner[In, Out, Ctx, K, M] =>
          setHandler(in, new MultiInHandler(multi))
      }

      setHandler(
        out,
        new OutHandler {
          override def onPull(): Unit = {
            if (outputBuffer.nonEmpty) {
              push(out, outputBuffer.remove(0))
            } else {
              if (isClosed(in)) stopIfNeeded()
              else pullIfNeeded()
            }
          }

          override def onDownstreamFinish(cause: Throwable): Unit = {
            failHandlersThenFailStage(cause)
            super.onDownstreamFinish(cause)
          }
        }
      )
    }

    override def createLogicAndMaterializedValue(
        inheritedAttributes: Attributes
      ): (GraphStageLogic, PartitionDynamic.Control[K, M]) = {
      val logic = new PartitionLogic(shape)
      val control = new PartitionDynamic.Control[K, M](
        logic.getKeysCallback,
        logic.spawnKeyCallback,
        logic.completeKeyCallback,
        logic.recreateKeyCallback,
        logic.withMaterializedValueCallback
      )

      (logic, control)
    }
  }
}
