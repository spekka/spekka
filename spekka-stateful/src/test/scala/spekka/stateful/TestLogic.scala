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

import akka.actor.ExtendedActorSystem
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorRefResolver
import akka.actor.typed.ActorSystem
import akka.actor.{ActorSystem => ClassicActorSystem}
import akka.pattern.StatusReply
import akka.serialization.Serializer
import akka.serialization.SerializerWithStringManifest

import scala.collection.immutable
import scala.concurrent.Future
import scala.concurrent.duration._

case class TestState(lastTimestamp: Long, counter: Long)

sealed trait TestCommand
case class GetCounter(replyTo: ActorRef[StatusReply[Long]]) extends TestCommand

class TestCommandSerializer(system: ExtendedActorSystem) extends SerializerWithStringManifest {

  private val actorRefResolver = ActorRefResolver(ActorSystem.wrap(system))

  override def identifier: Int = 11111

  override def manifest(o: AnyRef): String =
    o match {
      case _: GetCounter => "GetCounter"
      case _ => throw new IllegalArgumentException(s"Command not valid: ${o}")
    }

  override def toBinary(o: AnyRef): Array[Byte] =
    o match {
      case GetCounter(replyTo) => actorRefResolver.toSerializationFormat(replyTo).getBytes("UTF8")
      case _ => throw new IllegalArgumentException(s"Not a command: ${o}")
    }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef =
    manifest match {
      case "GetCounter" =>
        GetCounter(actorRefResolver.resolveActorRef(new String(bytes, "UTF8")))
      case _ => throw new IllegalArgumentException(s"Command not valid: ${manifest}")
    }
}

case class TestInput(timestamp: Long, discriminator: Int)
class TestInputSerializer extends Serializer {

  override def identifier: Int = 222222

  override def toBinary(o: AnyRef): Array[Byte] =
    o match {
      case TestInput(ts, discr) => s"$ts:$discr".getBytes("UTF8")
      case _ => throw new IllegalAccessException(s"Not a valid input: ${o}")
    }

  override def includeManifest: Boolean = false

  override def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
    val cmps = new String(bytes, "UTF8").split(":")

    TestInput(cmps(0).toLong, cmps(1).toInt)
  }
}

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

object EventBasedTestLogicAsync {
  def apply(
      initState: TestState,
      processingDelay: FiniteDuration,
      inputBeforeSideEffectsF: (TestState, TestInput) => immutable.Iterable[() => Future[_]] =
        (_, _) => Nil,
      inputAfterSideEffectsF: (TestState, TestInput) => immutable.Iterable[() => Future[_]] =
        (_, _) => Nil,
      commandBeforeSideEffectsF: (TestState, TestCommand) => immutable.Iterable[() => Future[_]] =
        (_, _) => Nil,
      commandAfterSideEffectsF: (TestState, TestCommand) => immutable.Iterable[() => Future[_]] =
        (_, _) => Nil
    )(implicit system: ClassicActorSystem
    ) =
    StatefulFlowLogic.EventBasedAsync(
      () => akka.pattern.after(100.millis)(Future.successful(initState)),
      (state: TestState, in: TestInput) =>
        akka.pattern.after(processingDelay)(
          Future.successful(
            EventBasedTestLogic
              .processInput(state, in, inputBeforeSideEffectsF, inputAfterSideEffectsF)
          )
        ),
      (state: TestState, event: TestEvent) => EventBasedTestLogic.updateState(state, event),
      (state: TestState, command: TestCommand) =>
        akka.pattern.after(processingDelay)(
          Future.successful(
            EventBasedTestLogic.processCommand(
              state,
              command,
              commandBeforeSideEffectsF,
              commandAfterSideEffectsF
            )
          )
        )
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

object DurableStateTestLogicAsync {
  def apply(
      initState: TestState,
      processingDelay: FiniteDuration,
      inputBeforeSideEffectsF: (TestState, TestInput) => immutable.Iterable[() => Future[_]] =
        (_, _) => Nil,
      inputAfterSideEffectsF: (TestState, TestInput) => immutable.Iterable[() => Future[_]] =
        (_, _) => Nil,
      commandBeforeSideEffectsF: (TestState, TestCommand) => immutable.Iterable[() => Future[_]] =
        (_, _) => Nil,
      commandAfterSideEffectsF: (TestState, TestCommand) => immutable.Iterable[() => Future[_]] =
        (_, _) => Nil
    )(implicit system: ClassicActorSystem
    ) =
    StatefulFlowLogic.DurableStateAsync(
      () => akka.pattern.after(100.millis)(Future.successful(initState)),
      (state: TestState, in: TestInput) =>
        akka.pattern.after(processingDelay)(
          Future.successful(
            DurableStateTestLogic
              .processInput(state, in, inputBeforeSideEffectsF, inputAfterSideEffectsF)
          )
        ),
      (state: TestState, command: TestCommand) =>
        akka.pattern.after(processingDelay)(
          Future.successful(
            DurableStateTestLogic
              .processCommand(state, command, commandBeforeSideEffectsF, commandAfterSideEffectsF)
          )
        )
    )
}
