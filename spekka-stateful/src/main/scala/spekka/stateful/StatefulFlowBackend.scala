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

import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success

sealed trait StatefulFlowBackend {
  val id: String
}

object StatefulFlowBackend {
  trait EventBased[State, Ev, BackendProtocol] extends StatefulFlowBackend {
    type Logic[In, Command] = StatefulFlowLogic.EventBased[State, Ev, In, Command]
    type Out = Ev

    def behaviorFor[In, Command](
        logic: StatefulFlowLogic.EventBased[State, Ev, In, Command],
        entityKind: String,
        entityId: String
      ): Behavior[StatefulFlowHandler.Protocol[In, Ev, Command, BackendProtocol]]

    def propsForLogic[In, Command](
        logic: Logic[In, Command]
      ): StatefulFlowProps[In, Out, Command] =
      new StatefulFlowProps.EventBased[State, Ev, In, Command, BackendProtocol](logic, this)
  }

  trait DurableState[State, BackendProtocol] extends StatefulFlowBackend {
    type Logic[In, Out, Command] = StatefulFlowLogic.DurableState[State, In, Out, Command]

    def behaviorFor[In, Out, Command](
        logic: StatefulFlowLogic.DurableState[State, In, Out, Command],
        entityKind: String,
        entityId: String
      ): Behavior[StatefulFlowHandler.Protocol[In, Out, Command, BackendProtocol]]

    def propsForLogic[In, Out, Command](
        logic: Logic[In, Out, Command]
      ): StatefulFlowProps[In, Out, Command] =
      new StatefulFlowProps.DurableState[State, In, Out, Command, BackendProtocol](logic, this)
  }

  object SideEffectHandlingBehavior {
    trait ProtocolAdapter[P <: StatefulFlowHandler.BackendProtocol[P]] {
      def buildSideEffectCompleteMessage: P
      def extractSideEffectComplete: PartialFunction[StatefulFlowHandler.Protocol[_, _, _, P], Unit]
      def buildSideEffectFailureMessage(error: Throwable): P
      def extractSideEffectFailure
          : PartialFunction[StatefulFlowHandler.Protocol[_, _, _, P], Throwable]
    }

    private[spekka] def apply[
        State,
        In,
        Out,
        Command,
        Protocol <: StatefulFlowHandler.BackendProtocol[Protocol]
      ](self: ActorRef[StatefulFlowHandler.Protocol[In, Out, Command, Protocol]],
        beforeUpdateSideEffects: Seq[() => Future[_]],
        beforeUpdateSideEffectsFailure: Throwable => Behavior[
          StatefulFlowHandler.Protocol[In, Out, Command, Protocol]
        ],
        afterUpdateSideEffects: Seq[() => Future[_]],
        afterUpdateSideEffectsFailure: Throwable => Behavior[
          StatefulFlowHandler.Protocol[In, Out, Command, Protocol]
        ],
        success: () => Behavior[
          StatefulFlowHandler.Protocol[In, Out, Command, Protocol]
        ],
        stashBufferSize: Int
      )(implicit ex: ExecutionContext,
        adapter: ProtocolAdapter[Protocol]
      ): Behavior[StatefulFlowHandler.Protocol[In, Out, Command, Protocol]] = {

      def handleBeforeSideEffect() = {
        Future.sequence(beforeUpdateSideEffects.map(_.apply())).onComplete {
          case Success(_) => self ! adapter.buildSideEffectCompleteMessage
          case Failure(ex) => self ! adapter.buildSideEffectFailureMessage(ex)
        }
        waitForBeforeSideEffect
      }

      def handleAfterSideEffect() = {
        Future.sequence(afterUpdateSideEffects.map(_.apply())).onComplete {
          case Success(_) => self ! adapter.buildSideEffectCompleteMessage
          case Failure(ex) => self ! adapter.buildSideEffectFailureMessage(ex)
        }
        waitForAfterSideEffect
      }

      def waitForBeforeSideEffect
          : Behavior[StatefulFlowHandler.Protocol[In, Out, Command, Protocol]] =
        Behaviors.withStash(stashBufferSize) { buffer =>
          Behaviors.receiveMessage { msg =>
            adapter.extractSideEffectComplete
              .andThen { _ =>
                if (afterUpdateSideEffects.nonEmpty) handleAfterSideEffect()
                else buffer.unstashAll(success())
              }
              .orElse {
                adapter.extractSideEffectFailure.andThen { ex =>
                  buffer.unstashAll(beforeUpdateSideEffectsFailure(ex))
                }
              }
              .applyOrElse(
                msg,
                { m: StatefulFlowHandler.Protocol[In, Out, Command, Protocol] =>
                  buffer.stash(m)
                  Behaviors.same
                }
              )
          }
        }

      def waitForAfterSideEffect
          : Behavior[StatefulFlowHandler.Protocol[In, Out, Command, Protocol]] =
        Behaviors.withStash(stashBufferSize) { buffer =>
          Behaviors.receiveMessage { msg =>
            adapter.extractSideEffectComplete
              .andThen { _ =>
                buffer.unstashAll(success())
              }
              .orElse {
                adapter.extractSideEffectFailure.andThen { ex =>
                  buffer.unstashAll(afterUpdateSideEffectsFailure(ex))
                }
              }
              .applyOrElse(
                msg,
                { m: StatefulFlowHandler.Protocol[In, Out, Command, Protocol] =>
                  buffer.stash(m)
                  Behaviors.same
                }
              )
          }
        }

      if (beforeUpdateSideEffects.nonEmpty) handleBeforeSideEffect()
      else if (afterUpdateSideEffects.nonEmpty) handleAfterSideEffect()
      else success()
    }

  }

}
