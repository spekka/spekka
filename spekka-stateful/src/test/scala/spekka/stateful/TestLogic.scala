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

import akka.actor.typed.ActorRef
import akka.pattern.StatusReply

import scala.collection.immutable
import scala.concurrent.Future

case class TestState(lastTimestamp: Long, counter: Long)

sealed trait TestCommand
case class GetCounter(replyTo: ActorRef[StatusReply[Long]]) extends TestCommand

case class TestInput(timestamp: Long, discriminator: Int)

sealed trait TestEvent
case class IncreaseCounterWithTimestamp(timestamp: Long) extends TestEvent

object EventBasedTestLogic {
  import StatefulFlowLogic.EventBased.ProcessingResult
  def processInput(
      state: TestState,
      in: TestInput,
      beforeSideEffectF: (TestState, TestInput) => immutable.Iterable[() => Future[_]],
      afterSideEffectF: (TestState, TestInput) => immutable.Iterable[() => Future[_]]
    ): ProcessingResult[TestEvent] = {
    if (in.timestamp > state.lastTimestamp)
      ProcessingResult
        .withEvent[TestEvent](
          IncreaseCounterWithTimestamp(in.timestamp)
        )
        .withBeforeUpdateSideEffects(beforeSideEffectF(state, in))
        .withAfterUpdateSideEffects(afterSideEffectF(state, in))
    else
      ProcessingResult.empty
  }

  def updateState(state: TestState, event: TestEvent): TestState = {
    event match {
      case IncreaseCounterWithTimestamp(timestamp) =>
        state.copy(lastTimestamp = timestamp, counter = state.counter + 1)
    }
  }

  def processCommand(
      state: TestState,
      command: TestCommand,
      beforeSideEffectF: (TestState, TestCommand) => immutable.Iterable[() => Future[_]],
      afterSideEffectF: (TestState, TestCommand) => immutable.Iterable[() => Future[_]]
    ): ProcessingResult[TestEvent] = {
    command match {
      case GetCounter(replyTo) =>
        replyTo ! StatusReply.success(state.counter)
        ProcessingResult.empty
          .withBeforeUpdateSideEffects(beforeSideEffectF(state, command))
          .withAfterUpdateSideEffects(afterSideEffectF(state, command))
    }
  }

  def apply(
      initState: TestState,
      inputBeforeSideEffectsF: (TestState, TestInput) => immutable.Iterable[() => Future[_]] =
        (_, _) => Nil,
      inputAfterSideEffectsF: (TestState, TestInput) => immutable.Iterable[() => Future[_]] =
        (_, _) => Nil,
      commandBeforeSideEffectsF: (TestState, TestCommand) => immutable.Iterable[() => Future[_]] =
        (_, _) => Nil,
      commandAfterSideEffectsF: (TestState, TestCommand) => immutable.Iterable[() => Future[_]] =
        (_, _) => Nil
    ) =
    StatefulFlowLogic.EventBased(
      () => initState,
      (state: TestState, in: TestInput) =>
        processInput(state, in, inputBeforeSideEffectsF, inputAfterSideEffectsF),
      (state: TestState, event: TestEvent) => updateState(state, event),
      (state: TestState, command: TestCommand) =>
        processCommand(state, command, commandBeforeSideEffectsF, commandAfterSideEffectsF)
    )
}

object DurableStateTestLogic {
  import StatefulFlowLogic.DurableState.ProcessingResult
  def processInput(
      state: TestState,
      in: TestInput,
      beforeSideEffectF: (TestState, TestInput) => immutable.Iterable[() => Future[_]],
      afterSideEffectF: (TestState, TestInput) => immutable.Iterable[() => Future[_]]
    ): ProcessingResult[TestState, TestEvent] = {
    if (in.timestamp > state.lastTimestamp) {
      val updatedState = state.copy(lastTimestamp = in.timestamp, counter = state.counter + 1)
      ProcessingResult[TestState, TestEvent](updatedState)
        .withOutput(IncreaseCounterWithTimestamp(in.timestamp))
        .withBeforeUpdateSideEffects(beforeSideEffectF(state, in))
        .withAfterUpdateSideEffects(afterSideEffectF(state, in))
    } else
      ProcessingResult(state)
  }

  def processCommand(
      state: TestState,
      command: TestCommand,
      beforeSideEffectF: (TestState, TestCommand) => immutable.Iterable[() => Future[_]],
      afterSideEffectF: (TestState, TestCommand) => immutable.Iterable[() => Future[_]]
    ): ProcessingResult[TestState, Nothing] = {
    command match {
      case GetCounter(replyTo) =>
        replyTo ! StatusReply.success(state.counter)
        ProcessingResult(state)
          .withBeforeUpdateSideEffects(beforeSideEffectF(state, command))
          .withAfterUpdateSideEffects(afterSideEffectF(state, command))
    }
  }

  def apply(
      initState: TestState,
      inputBeforeSideEffectsF: (TestState, TestInput) => immutable.Iterable[() => Future[_]] =
        (_, _) => Nil,
      inputAfterSideEffectsF: (TestState, TestInput) => immutable.Iterable[() => Future[_]] =
        (_, _) => Nil,
      commandBeforeSideEffectsF: (TestState, TestCommand) => immutable.Iterable[() => Future[_]] =
        (_, _) => Nil,
      commandAfterSideEffectsF: (TestState, TestCommand) => immutable.Iterable[() => Future[_]] =
        (_, _) => Nil
    ) =
    StatefulFlowLogic.DurableState(
      () => initState,
      (state: TestState, in: TestInput) =>
        processInput(state, in, inputBeforeSideEffectsF, inputAfterSideEffectsF),
      (state: TestState, command: TestCommand) =>
        processCommand(state, command, commandBeforeSideEffectsF, commandAfterSideEffectsF)
    )
}
