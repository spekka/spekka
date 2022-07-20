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
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.pattern.StatusReply

import scala.util.Failure
import scala.util.Success
import scala.util.Try

/** An in memory implementation of [[StatefulFlowBackend]].
  *
  * Useful for testing logics without an actually persisted storage.
  */
object InMemoryStatefulFlowBackend {
  sealed private[spekka] trait InMemoryBackendProtocol
      extends StatefulFlowHandler.BackendProtocol[InMemoryBackendProtocol]
  object InMemoryBackendProtocol {
    private[spekka] case object SideEffectCompleted extends InMemoryBackendProtocol
    private[spekka] case class SideEffectFailure(ex: Throwable) extends InMemoryBackendProtocol
  }

  sealed private[spekka] trait InMemoryBackendAsyncProtocol
      extends StatefulFlowHandler.BackendProtocol[InMemoryBackendAsyncProtocol]
  object InMemoryBackendAsyncProtocol {
    private[spekka] case class InputProcessingResultReady[P](
        result: Try[P],
        replyTo: ActorRef[StatusReply[StatefulFlowHandler.ProcessFlowOutput[Any]]])
        extends InMemoryBackendAsyncProtocol
    private[spekka] case class CommandProcessingResultReady[P](result: Try[P])
        extends InMemoryBackendAsyncProtocol
    private[spekka] case object SideEffectCompleted extends InMemoryBackendAsyncProtocol
    private[spekka] case class SideEffectFailure(ex: Throwable) extends InMemoryBackendAsyncProtocol
  }

  implicit private[spekka] val sideEffectHandlingBehaviorProtocolAdapter
      : StatefulFlowBackend.SideEffectHandlingBehavior.ProtocolAdapter[InMemoryBackendProtocol] =
    new StatefulFlowBackend.SideEffectHandlingBehavior.ProtocolAdapter[InMemoryBackendProtocol] {
      def buildSideEffectCompleteMessage: InMemoryBackendProtocol =
        InMemoryBackendProtocol.SideEffectCompleted

      def extractSideEffectComplete
          : PartialFunction[StatefulFlowHandler.Protocol[_, _, _, InMemoryBackendProtocol], Unit] = {
        case InMemoryBackendProtocol.SideEffectCompleted =>
      }

      def buildSideEffectFailureMessage(error: Throwable): InMemoryBackendProtocol =
        InMemoryBackendProtocol.SideEffectFailure(error)

      def extractSideEffectFailure
          : PartialFunction[StatefulFlowHandler.Protocol[_, _, _, InMemoryBackendProtocol], Throwable] = {
        case InMemoryBackendProtocol.SideEffectFailure(ex) => ex
      }
    }

  implicit private[spekka] val sideEffectHandlingBehaviorAsyncProtocolAdapter
      : StatefulFlowBackend.SideEffectHandlingBehavior.ProtocolAdapter[InMemoryBackendAsyncProtocol] =
    new StatefulFlowBackend.SideEffectHandlingBehavior.ProtocolAdapter[
      InMemoryBackendAsyncProtocol
    ] {
      def buildSideEffectCompleteMessage: InMemoryBackendAsyncProtocol =
        InMemoryBackendAsyncProtocol.SideEffectCompleted

      def extractSideEffectComplete
          : PartialFunction[StatefulFlowHandler.Protocol[_, _, _, InMemoryBackendAsyncProtocol], Unit] = {
        case InMemoryBackendAsyncProtocol.SideEffectCompleted =>
      }

      def buildSideEffectFailureMessage(error: Throwable): InMemoryBackendAsyncProtocol =
        InMemoryBackendAsyncProtocol.SideEffectFailure(error)

      def extractSideEffectFailure
          : PartialFunction[StatefulFlowHandler.Protocol[_, _, _, InMemoryBackendAsyncProtocol], Throwable] = {
        case InMemoryBackendAsyncProtocol.SideEffectFailure(ex) => ex
      }
    }

  /** An in memory [[StatefulFlowBackend.EventBased]] implementation
    */
  object EventBased {
    private[spekka] def behaviorFactory[State, Ev, In, Command](
        logic: StatefulFlowLogic.EventBased[State, Ev, In, Command],
        stashBufferSize: Int
      ): Behavior[StatefulFlowHandler.Protocol[In, Ev, Command, InMemoryBackendProtocol]] = {

      def behavior(
          state: State
        ): Behavior[StatefulFlowHandler.Protocol[In, Ev, Command, InMemoryBackendProtocol]] = {
        Behaviors.receive { (ctx, msg) =>
          msg match {
            case StatefulFlowHandler.ProcessFlowInput(in, replyTo) =>
              val result = logic.processInput(state, in)
              val updatedState = result.events.foldLeft(state)((s, ev) => logic.updateState(s, ev))

              StatefulFlowBackend.SideEffectHandlingBehavior(
                ctx.self,
                result.beforeUpdateSideEffects,
                ex => {
                  replyTo ! StatusReply.error(ex)
                  behavior(state)
                },
                result.afterUpdateSideEffects,
                ex => {
                  replyTo ! StatusReply.error(ex)
                  behavior(updatedState)
                },
                () => {
                  replyTo ! StatusReply.success(
                    StatefulFlowHandler.ProcessFlowOutput(result.events)
                  )
                  behavior(updatedState)
                },
                stashBufferSize
              )(ctx.executionContext, sideEffectHandlingBehaviorProtocolAdapter)

            case StatefulFlowHandler.ProcessCommand(command) =>
              val result = logic.processCommand(state, command)
              val updatedState = result.events.foldLeft(state)((s, ev) => logic.updateState(s, ev))

              StatefulFlowBackend.SideEffectHandlingBehavior(
                ctx.self,
                result.beforeUpdateSideEffects,
                _ => behavior(state),
                result.afterUpdateSideEffects,
                _ => behavior(updatedState),
                () => behavior(updatedState),
                stashBufferSize
              )(ctx.executionContext, sideEffectHandlingBehaviorProtocolAdapter)

            case StatefulFlowHandler.TerminateRequest(replyTo) =>
              replyTo ! StatusReply.ack()
              Behaviors.stopped

            case msg =>
              throw new IllegalArgumentException(
                s"Inconsistency in InMemoryStatefulFlowBackend.EventBased. Received unexpected message: $msg"
              )
          }
        }
      }

      behavior(logic.initialState)
    }

    /** Creates a new instance of [[InMemoryStatefulFlowBackend.EventBased]].
      *
      * @param sideEffectBufferSize
      *   Size of the buffer for stashing messages while performing side effects
      * @return
      *   [[StatefulFlowBackend.EventBased]] instance
      * @tparam State
      *   state type handled by this backend
      * @tparam Ev
      *   events type handled by this backend
      */
    def apply[State, Ev](
        sideEffectBufferSize: Int = 128
      ): StatefulFlowBackend.EventBased[State, Ev, InMemoryBackendProtocol] =
      new StatefulFlowBackend.EventBased[State, Ev, InMemoryBackendProtocol] {
        override val id: String = "in-memory-event-based"

        override private[spekka] def behaviorFor[In, Command](
            logic: StatefulFlowLogic.EventBased[State, Ev, In, Command],
            entityKind: String,
            entityId: String
          ): Behavior[StatefulFlowHandler.Protocol[In, Ev, Command, InMemoryBackendProtocol]] =
          behaviorFactory(logic, sideEffectBufferSize)
      }
  }

  object EventBasedAsync {
    private[spekka] def behaviorFactory[State, Ev, In, Command](
        logic: StatefulFlowLogic.EventBasedAsync[State, Ev, In, Command],
        stashBufferSize: Int
      ): Behavior[StatefulFlowHandler.Protocol[In, Ev, Command, InMemoryBackendAsyncProtocol]] = {

      def behavior(
          state: State
        ): Behavior[StatefulFlowHandler.Protocol[In, Ev, Command, InMemoryBackendAsyncProtocol]] = {
        Behaviors.receive { (ctx, msg) =>
          implicit val ec = ctx.executionContext

          msg match {
            case StatefulFlowHandler.ProcessFlowInput(in, replyTo) =>
              val self = ctx.self
              logic
                .processInput(state, in)
                .andThen { case res =>
                  self.tell(
                    InMemoryBackendAsyncProtocol.InputProcessingResultReady(res, replyTo)
                  )
                }
              Behaviors.same

            case InMemoryBackendAsyncProtocol.InputProcessingResultReady(
                  resultE: Try[
                    StatefulFlowLogic.EventBased.ProcessingResult[Ev]
                  ] @unchecked,
                  replyTo
                ) =>
              resultE match {
                case Success(result) =>
                  val updatedState =
                    result.events.foldLeft(state)((s, ev) => logic.updateState(s, ev))

                  StatefulFlowBackend.SideEffectHandlingBehavior(
                    ctx.self,
                    result.beforeUpdateSideEffects,
                    ex => {
                      replyTo ! StatusReply.error(ex)
                      behavior(state)
                    },
                    result.afterUpdateSideEffects,
                    ex => {
                      replyTo ! StatusReply.error(ex)
                      behavior(updatedState)
                    },
                    () => {
                      replyTo ! StatusReply.success(
                        StatefulFlowHandler.ProcessFlowOutput(result.events)
                      )
                      behavior(updatedState)
                    },
                    stashBufferSize
                  )(ctx.executionContext, sideEffectHandlingBehaviorAsyncProtocolAdapter)

                case Failure(ex) =>
                  replyTo ! StatusReply.error(ex)
                  Behaviors.same
              }

            case StatefulFlowHandler.ProcessCommand(command) =>
              val self = ctx.self
              logic
                .processCommand(state, command)
                .andThen { case res =>
                  self.tell(InMemoryBackendAsyncProtocol.CommandProcessingResultReady(res))
                }
              Behaviors.same

            case InMemoryBackendAsyncProtocol.CommandProcessingResultReady(
                  resultE: Try[StatefulFlowLogic.EventBased.ProcessingResult[Ev]] @unchecked
                ) =>
              resultE match {
                case Success(result) =>
                  val updatedState =
                    result.events.foldLeft(state)((s, ev) => logic.updateState(s, ev))

                  StatefulFlowBackend.SideEffectHandlingBehavior(
                    ctx.self,
                    result.beforeUpdateSideEffects,
                    _ => behavior(state),
                    result.afterUpdateSideEffects,
                    _ => behavior(updatedState),
                    () => behavior(updatedState),
                    stashBufferSize
                  )(ctx.executionContext, sideEffectHandlingBehaviorAsyncProtocolAdapter)

                case Failure(_) =>
                  Behaviors.same
              }

            case StatefulFlowHandler.TerminateRequest(replyTo) =>
              replyTo ! StatusReply.ack()
              Behaviors.stopped

            case msg =>
              throw new IllegalArgumentException(
                s"Inconsistency in InMemoryStatefulFlowBackend.EventBased. Received unexpected message: $msg"
              )
          }
        }
      }

      behavior(logic.initialState)
    }

    /** Creates a new instance of [[InMemoryStatefulFlowBackend.EventBased]].
      *
      * @param sideEffectBufferSize
      *   Size of the buffer for stashing messages while performing side effects
      * @return
      *   [[StatefulFlowBackend.EventBased]] instance
      * @tparam State
      *   state type handled by this backend
      * @tparam Ev
      *   events type handled by this backend
      */
    def apply[State, Ev](
        sideEffectBufferSize: Int = 128
      ): StatefulFlowBackend.EventBasedAsync[State, Ev, InMemoryBackendAsyncProtocol] =
      new StatefulFlowBackend.EventBasedAsync[State, Ev, InMemoryBackendAsyncProtocol] {
        override val id: String = "in-memory-event-based-async"

        override private[spekka] def behaviorFor[In, Command](
            logic: StatefulFlowLogic.EventBasedAsync[State, Ev, In, Command],
            entityKind: String,
            entityId: String
          ): Behavior[StatefulFlowHandler.Protocol[In, Ev, Command, InMemoryBackendAsyncProtocol]] =
          behaviorFactory(logic, sideEffectBufferSize)
      }
  }

  /** An in memory [[StatefulFlowBackend.DurableState]] implementation
    */
  object DurableState {
    private[spekka] def behaviorFactory[State, In, Out, Command](
        logic: StatefulFlowLogic.DurableState[State, In, Out, Command],
        stashBufferSize: Int
      ): Behavior[StatefulFlowHandler.Protocol[In, Out, Command, InMemoryBackendProtocol]] = {

      def behavior(
          state: State
        ): Behavior[StatefulFlowHandler.Protocol[In, Out, Command, InMemoryBackendProtocol]] = {
        Behaviors.receive { (ctx, msg) =>
          msg match {
            case StatefulFlowHandler.ProcessFlowInput(in, replyTo) =>
              val result = logic.processInput(state, in)
              val updatedState = result.state

              StatefulFlowBackend.SideEffectHandlingBehavior(
                ctx.self,
                result.beforeUpdateSideEffects,
                ex => {
                  replyTo ! StatusReply.error(ex)
                  behavior(state)
                },
                result.afterUpdateSideEffects,
                ex => {
                  replyTo ! StatusReply.error(ex)
                  behavior(updatedState)
                },
                () => {
                  replyTo ! StatusReply.success(StatefulFlowHandler.ProcessFlowOutput(result.outs))
                  behavior(updatedState)
                },
                stashBufferSize
              )(ctx.executionContext, sideEffectHandlingBehaviorProtocolAdapter)

            case StatefulFlowHandler.ProcessCommand(command) =>
              val result = logic.processCommand(state, command)
              val updatedState = result.state

              StatefulFlowBackend.SideEffectHandlingBehavior(
                ctx.self,
                result.beforeUpdateSideEffects,
                _ => behavior(state),
                result.afterUpdateSideEffects,
                _ => behavior(updatedState),
                () => behavior(updatedState),
                stashBufferSize
              )(ctx.executionContext, sideEffectHandlingBehaviorProtocolAdapter)

            case StatefulFlowHandler.TerminateRequest(replyTo) =>
              replyTo ! StatusReply.ack()
              Behaviors.stopped

            case msg =>
              throw new IllegalArgumentException(
                s"Inconsistency in InMemoryStatefulFlowBackend.DurableState. Received unexpected message: $msg"
              )
          }
        }
      }

      behavior(logic.initialState)
    }

    /** Creates a new instance of [[InMemoryStatefulFlowBackend.DurableState]].
      *
      * @param sideEffectBufferSize
      *   Size of the buffer for stashing messages while performing side effects
      * @return
      *   [[StatefulFlowBackend.DurableState]] instance
      * @tparam State
      *   state type handled by this backend
      */
    def apply[State](
        sideEffectBufferSize: Int = 128
      ): StatefulFlowBackend.DurableState[State, InMemoryBackendProtocol] =
      new StatefulFlowBackend.DurableState[State, InMemoryBackendProtocol] {
        override val id: String = "in-memory-durable-state"

        override private[spekka] def behaviorFor[In, Out, Command](
            logic: StatefulFlowLogic.DurableState[State, In, Out, Command],
            entityKind: String,
            entityId: String
          ): Behavior[StatefulFlowHandler.Protocol[In, Out, Command, InMemoryBackendProtocol]] =
          behaviorFactory(logic, sideEffectBufferSize)
      }
  }

  object DurableStateAsync {
    private[spekka] def behaviorFactory[State, In, Out, Command](
        logic: StatefulFlowLogic.DurableStateAsync[State, In, Out, Command],
        stashBufferSize: Int
      ): Behavior[StatefulFlowHandler.Protocol[In, Out, Command, InMemoryBackendAsyncProtocol]] = {

      def behavior(
          state: State
        ): Behavior[StatefulFlowHandler.Protocol[In, Out, Command, InMemoryBackendAsyncProtocol]] = {
        Behaviors.receive { (ctx, msg) =>
          implicit val ec = ctx.executionContext

          msg match {
            case StatefulFlowHandler.ProcessFlowInput(in, replyTo) =>
              val self = ctx.self
              logic
                .processInput(state, in)
                .andThen { case res =>
                  self.tell(
                    InMemoryBackendAsyncProtocol.InputProcessingResultReady(res, replyTo)
                  )
                }
              Behaviors.same

            case InMemoryBackendAsyncProtocol.InputProcessingResultReady(
                  resultE: Try[
                    StatefulFlowLogic.DurableState.ProcessingResult[State, Out]
                  ] @unchecked,
                  replyTo
                ) =>
              resultE match {
                case Success(result) =>
                  val updatedState = result.state

                  StatefulFlowBackend.SideEffectHandlingBehavior(
                    ctx.self,
                    result.beforeUpdateSideEffects,
                    ex => {
                      replyTo ! StatusReply.error(ex)
                      behavior(state)
                    },
                    result.afterUpdateSideEffects,
                    ex => {
                      replyTo ! StatusReply.error(ex)
                      behavior(updatedState)
                    },
                    () => {
                      replyTo ! StatusReply.success(
                        StatefulFlowHandler.ProcessFlowOutput(result.outs)
                      )
                      behavior(updatedState)
                    },
                    stashBufferSize
                  )(ctx.executionContext, sideEffectHandlingBehaviorAsyncProtocolAdapter)
                case Failure(ex) =>
                  replyTo ! StatusReply.error(ex)
                  Behaviors.same
              }

            case StatefulFlowHandler.ProcessCommand(command) =>
              val self = ctx.self
              logic
                .processCommand(state, command)
                .andThen { case res =>
                  self.tell(InMemoryBackendAsyncProtocol.CommandProcessingResultReady(res))
                }
              Behaviors.same

            case InMemoryBackendAsyncProtocol.CommandProcessingResultReady(
                  resultE: Try[
                    StatefulFlowLogic.DurableState.ProcessingResult[State, Out]
                  ] @unchecked
                ) =>
              resultE match {
                case Success(result) =>
                  val updatedState = result.state

                  StatefulFlowBackend.SideEffectHandlingBehavior(
                    ctx.self,
                    result.beforeUpdateSideEffects,
                    _ => behavior(state),
                    result.afterUpdateSideEffects,
                    _ => behavior(updatedState),
                    () => behavior(updatedState),
                    stashBufferSize
                  )(ctx.executionContext, sideEffectHandlingBehaviorAsyncProtocolAdapter)
                case Failure(_) =>
                  Behaviors.same
              }

            case StatefulFlowHandler.TerminateRequest(replyTo) =>
              replyTo ! StatusReply.ack()
              Behaviors.stopped

            case msg =>
              throw new IllegalArgumentException(
                s"Inconsistency in InMemoryStatefulFlowBackend.DurableState. Received unexpected message: $msg"
              )
          }
        }
      }

      behavior(logic.initialState)
    }

    /** Creates a new instance of [[InMemoryStatefulFlowBackend.DurableState]].
      *
      * @param sideEffectBufferSize
      *   Size of the buffer for stashing messages while performing side effects
      * @return
      *   [[StatefulFlowBackend.DurableState]] instance
      * @tparam State
      *   state type handled by this backend
      */
    def apply[State](
        sideEffectBufferSize: Int = 128
      ): StatefulFlowBackend.DurableStateAsync[State, InMemoryBackendAsyncProtocol] =
      new StatefulFlowBackend.DurableStateAsync[State, InMemoryBackendAsyncProtocol] {
        override val id: String = "in-memory-durable-state-async"

        override private[spekka] def behaviorFor[In, Out, Command](
            logic: StatefulFlowLogic.DurableStateAsync[State, In, Out, Command],
            entityKind: String,
            entityId: String
          ): Behavior[StatefulFlowHandler.Protocol[In, Out, Command, InMemoryBackendAsyncProtocol]] =
          behaviorFactory(logic, sideEffectBufferSize)
      }
  }
}
