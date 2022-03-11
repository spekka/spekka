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

package spekka.test

import akka.NotUsed
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.stream.Attributes
import akka.stream.FlowShape
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.scaladsl.Flow
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler
import akka.testkit.TestProbe

import scala.collection.mutable

object SeqFlow {
  case class UpstreamFinished[T, Ctx](data: Seq[(T, Ctx)])
  case class UpstreamFailure(ex: Throwable)
  case class DownstreamFinished(ex: Throwable)

  class SeqFlowProbe[T, Ctx](val probe: TestProbe) {
    def expectUpstreamFinished(): Seq[(T, Ctx)] = {
      probe.expectMsgType[UpstreamFinished[T, Ctx]].data
    }
    def expectUpstreamFailure(): Throwable = {
      probe.expectMsgType[UpstreamFailure].ex
    }
    def expectDownstreamFinished(): Throwable = {
      probe.expectMsgType[DownstreamFinished].ex
    }
  }

  def apply[T, Ctx](
    )(implicit system: ActorSystem
    ): (SeqFlowProbe[T, Ctx], Flow[(T, Ctx), (T, Ctx), NotUsed]) = {
    val probe = TestProbe()
    val flow = Flow.fromGraph(new SeqFlow[T, Ctx](probe.ref))
    new SeqFlowProbe(probe) -> flow
  }
}

class SeqFlow[T, Ctx](probe: ActorRef) extends GraphStage[FlowShape[(T, Ctx), (T, Ctx)]] {

  val in: Inlet[(T, Ctx)] = Inlet("SeqFlow.in")
  val out: Outlet[(T, Ctx)] = Outlet("SeqFlow.out")
  override def shape: FlowShape[(T, Ctx), (T, Ctx)] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      val acc: mutable.ListBuffer[(T, Ctx)] = mutable.ListBuffer()

      setHandler(
        in,
        new InHandler() {
          override def onPush(): Unit = {
            val data = grab(in)
            acc += data
            push(out, data)
          }

          override def onUpstreamFailure(ex: Throwable): Unit = {
            probe.tell(SeqFlow.UpstreamFailure(ex), null)
            super.onUpstreamFailure(ex)
          }

          override def onUpstreamFinish(): Unit = {
            probe.tell(SeqFlow.UpstreamFinished(acc.toList), null)
            super.onUpstreamFinish()
          }
        }
      )

      setHandler(
        out,
        new OutHandler() {
          override def onPull(): Unit = pull(in)

          override def onDownstreamFinish(cause: Throwable): Unit = {
            probe.tell(SeqFlow.DownstreamFinished(cause), null)
            super.onDownstreamFinish(cause)
          }
        }
      )
    }
}
