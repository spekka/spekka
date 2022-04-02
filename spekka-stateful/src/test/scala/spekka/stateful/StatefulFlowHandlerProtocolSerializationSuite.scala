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

package spekka.stateful

import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.testkit.TestProbe
import spekka.test.SpekkaSuite

import scala.concurrent.Future
import scala.concurrent.duration._

object StatefulFlowHandlerProtocolSerializationSuite {
  val config = """
    | akka.actor {
    |   serialize-messages = on
    |   no-serialization-verification-needed-class-prefix = [
    |     "akka.",
    |     "spekka.stateful.StatefulFlowHandlerProtocolSerializationSuite",
    |     "spekka.stateful.StatefulFlowRegistry",
    |     "spekka.stateful.InMemoryStatefulFlowBackend"
    |   ]
    |   
    |   serializers {
    |     test-command = "spekka.stateful.TestCommandSerializer"
    |     test-input = "spekka.stateful.TestInputSerializer"
    |   }
    |
    |  serialization-bindings {
    |    "spekka.stateful.TestCommand" = test-command
    |    "spekka.stateful.TestInput" = test-input
    |  }
    | }
    | """.stripMargin
}

class StatefulFlowHandlerProtocolSerializationSuite
    extends SpekkaSuite(
      "StatefulFlowHandlerProtocolSerialization",
      StatefulFlowHandlerProtocolSerializationSuite.config
    ) {

  import scala.concurrent.ExecutionContext.Implicits.global

  case class TestEffect(name: String, ts: Long)

  val inputs = 1.to(10).map(i => TestInput(i.toLong, 1)) :+ TestInput(0L, 1)
  val registry = StatefulFlowRegistry(30.seconds)

  test("simple flow with side effects") {
    val inputSideEffectsProbe = TestProbe()
    val commandSideEffectsProbe = TestProbe()

    val flowProps = EventBasedTestLogic(
      TestState(0, 0),
      inputBeforeSideEffectsF = (state, input) => {
        if (input.timestamp == 5L)
          List(() =>
            Future.successful(inputSideEffectsProbe.ref ! TestEffect("before", state.lastTimestamp))
          )
        else Nil
      },
      inputAfterSideEffectsF = (state, input) => {
        if (input.timestamp == 5L)
          List(() =>
            Future.successful(inputSideEffectsProbe.ref ! TestEffect("after", state.lastTimestamp))
          )
        else Nil
      },
      commandBeforeSideEffectsF = (_, _) => {
        List(() => Future.successful(commandSideEffectsProbe.ref ! TestEffect("before", 0)))
      },
      commandAfterSideEffectsF = (_, _) => {
        List(() => Future.successful(commandSideEffectsProbe.ref ! TestEffect("after", 0)))
      }
    ).propsForBackend(InMemoryStatefulFlowBackend.EventBased())

    val resultF = for {
      builder <- registry.registerStatefulFlow("testKind-event-effects", flowProps)

      (controlF, resF) = Source(inputs)
        .viaMat(builder.flow("1"))(Keep.right)
        .toMat(Sink.seq)(Keep.both)
        .run()

      _ = inputSideEffectsProbe.expectMsg(TestEffect("before", 4L))
      _ = inputSideEffectsProbe.expectMsg(TestEffect("after", 4L))

      res <- resF
      control <- controlF

      _ = commandSideEffectsProbe.expectNoMessage()
      counter <- control.commandWithResult(GetCounter(_))
      _ = commandSideEffectsProbe.expectMsg(TestEffect("before", 0))
      _ = commandSideEffectsProbe.expectMsg(TestEffect("after", 0))

      _ <- control.terminate()
    } yield (counter, res.flatten)

    val (counter, events) = resultF.futureValue

    counter shouldBe 10L
    events shouldBe 1.to(10).map(i => IncreaseCounterWithTimestamp(i.toLong))
  }
}
