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

/** A [[StatefulFlowBackend]] is responsible of managing the persistence of the state of a stateful
  * flow.
  *
  * It is not concerned to how/when the state is changed (which is controlled by the
  * [[StatefulFlowLogic]]), but is the ultimate component in charge of making sure that the state is
  * actually persisted and recovered.
  *
  * The currently supported stateful flow backends are:
  *
  *   - [[StatefulFlowBackend.EventBased]]
  *   - [[StatefulFlowBackend.DurableState]]
  */
sealed trait StatefulFlowBackend {

  /** Unique identifier of the backend implementation
    */
  val id: String
}

/** Namespace object for stateful flow backends
  */
object StatefulFlowBackend {

  /** A [[StatefulFlowBackend]] for [[StatefulFlowLogic.EventBased]] logics.
    *
    * The state is defined by the series of event that generated it. The backend is responsible of
    * persisting the individual events and recompute the state by re-applying the persisted events
    * starting from the initial empty state.
    *
    * @tparam State
    *   the type of state managed by the backend
    * @tparam Ev
    *   the type of event managed by the backend
    * @tparam BackendProtocol
    *   the internal protocol of the backend
    */
  trait EventBased[State, Ev, BackendProtocol] extends StatefulFlowBackend {

    /** The logic type compatible with this backend
      */
    type Logic[In, Command] = StatefulFlowLogic.EventBased[State, Ev, In, Command]

    private[spekka] def behaviorFor[In, Command](
        logic: StatefulFlowLogic.EventBased[State, Ev, In, Command],
        entityKind: String,
        entityId: String
      ): Behavior[StatefulFlowHandler.Protocol[In, Ev, Command, BackendProtocol]]

    /** Creates a [[StatefulFlowProps]] for this backend with the specified logic.
      *
      * @param logic
      *   An [[StatefulFlowLogic.EventBased]] logic to use together with this backend to create a
      *   stateful flow
      * @return
      *   [[StatefulFlowProps]] for this backend and the specified logic
      */
    def propsForLogic[In, Command](
        logic: Logic[In, Command]
      ): StatefulFlowProps[In, Ev, Command] =
      new StatefulFlowProps.EventBased[State, Ev, In, Command, BackendProtocol](logic, this)
  }

  trait EventBasedAsync[State, Ev, BackendProtocol] extends StatefulFlowBackend {

    /** The logic type compatible with this backend
      */
    type Logic[In, Command] = StatefulFlowLogic.EventBasedAsync[State, Ev, In, Command]

    private[spekka] def behaviorFor[In, Command](
        logic: StatefulFlowLogic.EventBasedAsync[State, Ev, In, Command],
        entityKind: String,
        entityId: String
      ): Behavior[StatefulFlowHandler.Protocol[In, Ev, Command, BackendProtocol]]

    /** Creates a [[StatefulFlowProps]] for this backend with the specified logic.
      *
      * @param logic
      *   An [[StatefulFlowLogic.EventBased]] logic to use together with this backend to create a
      *   stateful flow
      * @return
      *   [[StatefulFlowProps]] for this backend and the specified logic
      */
    def propsForLogic[In, Command](
        logic: Logic[In, Command]
      ): StatefulFlowProps[In, Ev, Command] =
      new StatefulFlowProps.EventBasedAsync[State, Ev, In, Command, BackendProtocol](logic, this)
  }

  /** A [[StatefulFlowBackend]] for [[StatefulFlowLogic.DurableState]] logics.
    *
    * @tparam State
    *   the type of state managed by the backend
    * @tparam BackendProtocol
    *   the internal protocol of the backend
    */
  trait DurableState[State, BackendProtocol] extends StatefulFlowBackend {

    /** The logic type compatible with this backend
      */
    type Logic[In, Out, Command] = StatefulFlowLogic.DurableState[State, In, Out, Command]

    private[spekka] def behaviorFor[In, Out, Command](
        logic: StatefulFlowLogic.DurableState[State, In, Out, Command],
        entityKind: String,
        entityId: String
      ): Behavior[StatefulFlowHandler.Protocol[In, Out, Command, BackendProtocol]]

    /** Creates a [[StatefulFlowProps]] for this backend with the specified logic.
      *
      * @param logic
      *   An [[StatefulFlowLogic.EventBased]] logic to use together with this backend to create a
      *   stateful flow
      * @return
      *   [[StatefulFlowProps]] for this backend and the specified logic
      */
    def propsForLogic[In, Out, Command](
        logic: Logic[In, Out, Command]
      ): StatefulFlowProps[In, Out, Command] =
      new StatefulFlowProps.DurableState[State, In, Out, Command, BackendProtocol](logic, this)
  }

  trait DurableStateAsync[State, BackendProtocol] extends StatefulFlowBackend {

    /** The logic type compatible with this backend
      */
    type Logic[In, Out, Command] = StatefulFlowLogic.DurableStateAsync[State, In, Out, Command]

    private[spekka] def behaviorFor[In, Out, Command](
        logic: StatefulFlowLogic.DurableStateAsync[State, In, Out, Command],
        entityKind: String,
        entityId: String
      ): Behavior[StatefulFlowHandler.Protocol[In, Out, Command, BackendProtocol]]

    /** Creates a [[StatefulFlowProps]] for this backend with the specified logic.
      *
      * @param logic
      *   An [[StatefulFlowLogic.EventBased]] logic to use together with this backend to create a
      *   stateful flow
      * @return
      *   [[StatefulFlowProps]] for this backend and the specified logic
      */
    def propsForLogic[In, Out, Command](
        logic: Logic[In, Out, Command]
      ): StatefulFlowProps[In, Out, Command] =
      new StatefulFlowProps.DurableStateAsync[State, In, Out, Command, BackendProtocol](logic, this)
  }

  private[spekka] object SideEffectHandlingBehavior {
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
