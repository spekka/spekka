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

object InMemoryStatefulFlowSuite {
  val config = """
    | akka.stream.materializer.initial-input-buffer-size = 2
    | akka.stream.materializer.max-input-buffer-size = 2
    | """.stripMargin
}

class InMemoryStatefulFlowSuite
    extends SpekkaSuite("InMemoryStatefulFlow", InMemoryStatefulFlowSuite.config) {

  import scala.concurrent.ExecutionContext.Implicits.global

  val inputs = 1.to(10).map(i => TestInput(i.toLong, 1)) :+ TestInput(0L, 1)
  val registry = StatefulFlowRegistry(30.seconds)

  test("event based - simple flow, no side effects") {
    val flowProps = EventBasedTestLogic(TestState(0, 0))
      .propsForBackend(InMemoryStatefulFlowBackend.EventBased())

    val resultF = for {
      builder <- registry.registerStatefulFlow("testKind-event", flowProps)

      (controlF, resF) = Source(inputs)
        .viaMat(builder.flow("1"))(Keep.right)
        .toMat(Sink.seq)(Keep.both)
        .run()

      res <- resF
      control <- controlF

      counter <- control.commandWithResult(GetCounter(_))
      _ <- control.terminate()
    } yield (counter, res.flatten)

    val (counter, events) = resultF.futureValue
    counter shouldBe 10L
    events shouldBe 1.to(10).map(i => IncreaseCounterWithTimestamp(i.toLong))
  }

  test("event based - simple flow with side effects") {
    val inputSideEffectsProbe = TestProbe()
    val commandSideEffectsProbe = TestProbe()

    val flowProps = EventBasedTestLogic(
      TestState(0, 0),
      inputBeforeSideEffectsF = (state, input) => {
        if (input.timestamp == 5L)
          List(() =>
            Future.successful(inputSideEffectsProbe.ref ! ("before" -> state.lastTimestamp))
          )
        else Nil
      },
      inputAfterSideEffectsF = (state, input) => {
        if (input.timestamp == 5L)
          List(() =>
            Future.successful(inputSideEffectsProbe.ref ! ("after" -> state.lastTimestamp))
          )
        else Nil
      },
      commandBeforeSideEffectsF = (_, _) => {
        List(() => Future.successful(commandSideEffectsProbe.ref ! "before"))
      },
      commandAfterSideEffectsF = (_, _) => {
        List(() => Future.successful(commandSideEffectsProbe.ref ! "after"))
      }
    ).propsForBackend(InMemoryStatefulFlowBackend.EventBased())

    val resultF = for {
      builder <- registry.registerStatefulFlow("testKind-event-effects", flowProps)

      (controlF, resF) = Source(inputs)
        .viaMat(builder.flow("1"))(Keep.right)
        .toMat(Sink.seq)(Keep.both)
        .run()

      _ = inputSideEffectsProbe.expectMsg("before" -> 4L)
      _ = inputSideEffectsProbe.expectMsg("after" -> 4L)

      res <- resF
      control <- controlF

      _ = commandSideEffectsProbe.expectNoMessage()
      counter <- control.commandWithResult(GetCounter(_))
      _ = commandSideEffectsProbe.expectMsg("before")
      _ = commandSideEffectsProbe.expectMsg("after")

      _ <- control.terminate()
    } yield (counter, res.flatten)

    val (counter, events) = resultF.futureValue

    counter shouldBe 10L
    events shouldBe 1.to(10).map(i => IncreaseCounterWithTimestamp(i.toLong))
  }

  test("event based async - simple flow with side effects") {
    val inputSideEffectsProbe = TestProbe()
    val commandSideEffectsProbe = TestProbe()

    val flowProps = EventBasedTestLogicAsync(
      TestState(0, 0),
      100.millis,
      inputBeforeSideEffectsF = (state, input) => {
        if (input.timestamp == 5L)
          List(() =>
            Future.successful(inputSideEffectsProbe.ref ! ("before" -> state.lastTimestamp))
          )
        else Nil
      },
      inputAfterSideEffectsF = (state, input) => {
        if (input.timestamp == 5L)
          List(() =>
            Future.successful(inputSideEffectsProbe.ref ! ("after" -> state.lastTimestamp))
          )
        else Nil
      },
      commandBeforeSideEffectsF = (_, _) => {
        List(() => Future.successful(commandSideEffectsProbe.ref ! "before"))
      },
      commandAfterSideEffectsF = (_, _) => {
        List(() => Future.successful(commandSideEffectsProbe.ref ! "after"))
      }
    )(system).propsForBackend(InMemoryStatefulFlowBackend.EventBasedAsync())

    val resultF = for {
      builder <- registry.registerStatefulFlow("testKind-event-async-effects", flowProps)

      (controlF, resF) = Source(inputs)
        .viaMat(builder.flow("1"))(Keep.right)
        .toMat(Sink.seq)(Keep.both)
        .run()

      _ = inputSideEffectsProbe.expectMsg("before" -> 4L)
      _ = inputSideEffectsProbe.expectMsg("after" -> 4L)

      res <- resF
      control <- controlF

      _ = commandSideEffectsProbe.expectNoMessage()
      counter <- control.commandWithResult(GetCounter(_))
      _ = commandSideEffectsProbe.expectMsg("before")
      _ = commandSideEffectsProbe.expectMsg("after")

      _ <- control.terminate()
    } yield (counter, res.flatten)

    val (counter, events) = resultF.futureValue

    counter shouldBe 10L
    events shouldBe 1.to(10).map(i => IncreaseCounterWithTimestamp(i.toLong))
  }

  test("durable state - simple flow no side effects") {
    val flowProps = DurableStateTestLogic(TestState(0, 0))
      .propsForBackend(InMemoryStatefulFlowBackend.DurableState())

    val resultF = for {
      builder <- registry.registerStatefulFlow("testKind-durable", flowProps)

      (controlF, resF) = Source(inputs)
        .viaMat(builder.flow("1"))(Keep.right)
        .toMat(Sink.seq)(Keep.both)
        .run()

      res <- resF
      control <- controlF

      counter <- control.commandWithResult(GetCounter(_))
      _ <- control.terminate()
    } yield (counter, res.flatten)

    val (counter, events) = resultF.futureValue
    counter shouldBe 10L
    events shouldBe 1.to(10).map(i => IncreaseCounterWithTimestamp(i.toLong))
  }

  test("durable state - simple flow with side effects") {
    val inputSideEffectsProbe = TestProbe()
    val commandSideEffectsProbe = TestProbe()

    val flowProps = DurableStateTestLogic(
      TestState(0, 0),
      inputBeforeSideEffectsF = (state, input) => {
        if (input.timestamp == 5L)
          List(() =>
            Future.successful(inputSideEffectsProbe.ref ! ("before" -> state.lastTimestamp))
          )
        else Nil
      },
      inputAfterSideEffectsF = (state, input) => {
        if (input.timestamp == 5L)
          List(() =>
            Future.successful(inputSideEffectsProbe.ref ! ("after" -> state.lastTimestamp))
          )
        else Nil
      },
      commandBeforeSideEffectsF = (_, _) => {
        List(() => Future.successful(commandSideEffectsProbe.ref ! "before"))
      },
      commandAfterSideEffectsF = (_, _) => {
        List(() => Future.successful(commandSideEffectsProbe.ref ! "after"))
      }
    ).propsForBackend(InMemoryStatefulFlowBackend.DurableState())

    val resultF = for {
      builder <- registry.registerStatefulFlow("testKind-durable-effects", flowProps)

      (controlF, resF) = Source(inputs)
        .viaMat(builder.flow("1"))(Keep.right)
        .toMat(Sink.seq)(Keep.both)
        .run()

      _ = inputSideEffectsProbe.expectMsg("before" -> 4L)
      _ = inputSideEffectsProbe.expectMsg("after" -> 4L)

      res <- resF
      control <- controlF

      _ = commandSideEffectsProbe.expectNoMessage()
      counter <- control.commandWithResult(GetCounter(_))
      _ = commandSideEffectsProbe.expectMsg("before")
      _ = commandSideEffectsProbe.expectMsg("after")

      _ <- control.terminate()
    } yield (counter, res.flatten)

    val (counter, events) = resultF.futureValue
    counter shouldBe 10L
    events shouldBe 1.to(10).map(i => IncreaseCounterWithTimestamp(i.toLong))
  }

  test("durable state async - simple flow with side effects") {
    val inputSideEffectsProbe = TestProbe()
    val commandSideEffectsProbe = TestProbe()

    val flowProps = DurableStateTestLogicAsync(
      TestState(0, 0),
      100.millis,
      inputBeforeSideEffectsF = (state, input) => {
        if (input.timestamp == 5L)
          List(() =>
            Future.successful(inputSideEffectsProbe.ref ! ("before" -> state.lastTimestamp))
          )
        else Nil
      },
      inputAfterSideEffectsF = (state, input) => {
        if (input.timestamp == 5L)
          List(() =>
            Future.successful(inputSideEffectsProbe.ref ! ("after" -> state.lastTimestamp))
          )
        else Nil
      },
      commandBeforeSideEffectsF = (_, _) => {
        List(() => Future.successful(commandSideEffectsProbe.ref ! "before"))
      },
      commandAfterSideEffectsF = (_, _) => {
        List(() => Future.successful(commandSideEffectsProbe.ref ! "after"))
      }
    )(system).propsForBackend(InMemoryStatefulFlowBackend.DurableStateAsync())

    val resultF = for {
      builder <- registry.registerStatefulFlow("testKind-durable-async-effects", flowProps)

      (controlF, resF) = Source(inputs)
        .viaMat(builder.flow("1"))(Keep.right)
        .toMat(Sink.seq)(Keep.both)
        .run()

      _ = inputSideEffectsProbe.expectMsg("before" -> 4L)
      _ = inputSideEffectsProbe.expectMsg("after" -> 4L)

      res <- resF
      control <- controlF

      _ = commandSideEffectsProbe.expectNoMessage()
      counter <- control.commandWithResult(GetCounter(_))
      _ = commandSideEffectsProbe.expectMsg("before")
      _ = commandSideEffectsProbe.expectMsg("after")

      _ <- control.terminate()
    } yield (counter, res.flatten)

    val (counter, events) = resultF.futureValue
    counter shouldBe 10L
    events shouldBe 1.to(10).map(i => IncreaseCounterWithTimestamp(i.toLong))
  }
}
