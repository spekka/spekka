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

import scala.collection.immutable
import scala.concurrent.Future

/**
  * A [[StatefulFlowLogic]] is responsible of defining how inputs and commands affect the state.
  *
  * The objective is to provide a clear separation of how the state changes in response to input/commands
  * and how the state is managed in the backend.
  *
  * This decoupling makes the code testable and makes it easier to change the persistence layer of the state.
  *
  * A [[StatefulFlowLogic]] needs to describe what happens when the flow it's associated to receive an input:
  * how the state should be modified and what outputs needs to be produced?
  *
  * Furthermore there is the possibility to define commands that are received out of band w.r.t. the stream which
  * may affect the state or may be simply used to provide a mechanism to query the current state.
  *
  * The currently supported stateful flow logic strategies are:
  *
  *   - [[StatefulFlowLogic.EventBased]]
  *   - [[StatefulFlowLogic.DurableState]]
  *
  *
  * @tparam State the type of state managed by this logic
  * @tparam In the type of input accepted by this logic
  * @tparam Command the type of command accepted by this logic
  * @tparam Out the type of outputs generated by this logic
  */
sealed trait StatefulFlowLogic[State, +In, -Out, Command] {
  private[spekka] def initialState: State
}


/**
  * Namespace object for stateful flow logics
  */
object StatefulFlowLogic {
  /**
    * An empty type used to describe stateful flow logic not accepting any command
    */
  sealed trait NoCommand

  /**
    * A [[StatefulFlowLogic]] based on events.
    *
    * The idea is that inputs/commands do not change directly the state, but instead the logic 
    * may generate a series of events upon receiving an input/command. These events will then be
    * used to modify the state and will be produced as output.
    *
    * This kind of logic is well suited to implement event-sourced services.
    *
    * @tparam State the type of state handled by the logic
    * @tparam Ev the type of events generated by the logic
    * @tparam In the type of input handled by the logic
    * @tparam Command the type of commands handled by the logic
    */
  trait EventBased[State, Ev, In, Command] extends StatefulFlowLogic[State, In, Ev, Command] {
    private[spekka] def processCommand(
        state: State,
        command: Command
      ): EventBased.ProcessingResult[Ev]
    private[spekka] def processInput(state: State, in: In): EventBased.ProcessingResult[Ev]
    private[spekka] def updateState(state: State, event: Ev): State

    /**
      * Creates a [[StatefulFlowProps]] for this logic with the specified backend.
      *
      * @param backend An [[StatefulFlowBackend.EventBased]] backend to use together with this logic to create a stateful flow
      * @return [[StatefulFlowProps]] for this logic and the specified backend
      */
    def propsForBackend[Protocol](
        backend: StatefulFlowBackend.EventBased[State, Ev, Protocol]
      ): StatefulFlowProps[In, Ev, Command] = backend.propsForLogic(this)
  }

  /**
    * Namespace object for [[EventBased]] stateful flow logics.
    */
  object EventBased {

    /**
      * Represent the result of processing an input/command by an [[EventBased]] stateful flow logic.
      *
      * The result may contain zero or multiple events to be applied to the state and may specify
      * a series of side-effects to be performed before and after the events are applied to the state.
      *
      * Is is guaranteed that all `beforeUpdateSideEffects` will be completed successfully before the state
      * is updated with the events. Furthermore it is guaranteed that `afterUpdateSideEffects` will be executed
      * only after the state has been successfully updated with all the events in this result.
      *
      * In case the side effects or the state update fail, the flow associated to this logic will fail.
      */
    class ProcessingResult[Ev] private[ProcessingResult] (
        private[spekka] val events: Vector[Ev],
        private[spekka] val beforeUpdateSideEffects: Vector[() => Future[_]],
        private[spekka] val afterUpdateSideEffects: Vector[() => Future[_]]) {

      /**
        * Checks whether this result has side effects
        *
        * @return true if there are side effect in this result
        */
      def hasSideEffects: Boolean =
        beforeUpdateSideEffects.nonEmpty || afterUpdateSideEffects.nonEmpty

      /**
        * Append the specified event to the list of events used to update the state.
        *
        * @param ev the event to append
        * @return updated result
        */
      def withEvent(ev: Ev): ProcessingResult[Ev] =
        new ProcessingResult(events :+ ev, beforeUpdateSideEffects, afterUpdateSideEffects)

      /**
        * Append all the specified events to the list of events used to update the state.
        *
        * @param ev the events to append
        * @return updated result
        */
      def withEvents(ev: immutable.Seq[Ev]): ProcessingResult[Ev] =
        new ProcessingResult(events ++ ev, beforeUpdateSideEffects, afterUpdateSideEffects)

      /**
        * Append a side effect to the list of side effects to perform before updating the state.
        *
        * @param sideEffect the side effect to append
        * @return updated result
        */
      def withBeforeUpdateSideEffect(sideEffect: () => Future[_]): ProcessingResult[Ev] =
        new ProcessingResult(events, beforeUpdateSideEffects :+ sideEffect, afterUpdateSideEffects)

      /**
        * Append all the side effects to the list of side effects to perform before updating the state.
        *
        * @param sideEffects the side effects to append
        * @return updated result
        */
      def withBeforeUpdateSideEffects(
          sideEffects: immutable.Iterable[() => Future[_]]
        ): ProcessingResult[Ev] =
        new ProcessingResult(events, beforeUpdateSideEffects ++ sideEffects, afterUpdateSideEffects)

      /**
        * Append a side effect to the list of side effects to perform after the state has been updated.
        *
        * @param sideEffect the side effect to append
        * @return updated result
        */
      def withAfterUpdateSideEffect(sideEffect: () => Future[_]): ProcessingResult[Ev] =
        new ProcessingResult(events, beforeUpdateSideEffects, afterUpdateSideEffects :+ sideEffect)

      /**
        * Append all the side effects to the list of side effects to perform after the state has been updated.
        *
        * @param sideEffects the side effects to append
        * @return updated result
        */
      def withAfterUpdateSideEffects(
          sideEffects: immutable.Iterable[() => Future[_]]
        ): ProcessingResult[Ev] =
        new ProcessingResult(events, beforeUpdateSideEffects, afterUpdateSideEffects ++ sideEffects)

      /**
        * Combine this result with the other result.
        *
        * The combined result will perform in order:
        *
        * - perform `this.beforeUpdateSideEffects`
        * - perform `other.beforeUpdateSideEffects`
        * - update state with `this.events`
        * - update state with `other.events`
        * - perform `this.afterUpdateSideEffects`
        * - perform `other.afterUpdateSideEffects`
        *
        * @param other the other result to combine with this instance
        * @return combined result
        */
      def combine(other: ProcessingResult[Ev]): ProcessingResult[Ev] =
        new ProcessingResult(
          events ++ other.events,
          beforeUpdateSideEffects ++ other.beforeUpdateSideEffects,
          afterUpdateSideEffects ++ other.afterUpdateSideEffects
        )
    }

    /**
      * Processing result builder
      */
    object ProcessingResult {
      /**
        * Create a result with no events and no side effects.
        *
        * @return processing result
        */
      def empty[Ev]: ProcessingResult[Ev] =
        new ProcessingResult(Vector.empty, Vector.empty, Vector.empty)

      /**
        * Create a result with the specified event and no side effects
        *
        * @param ev the event to produce
        * @return processing result
        */
      def withEvent[Ev](ev: Ev): ProcessingResult[Ev] =
        new ProcessingResult(Vector(ev), Vector.empty, Vector.empty)

      /**
        * Create a result with the specified events and no side effects
        *
        * @param evs the events to produce
        * @return processing result
        */
      def withEvents[Ev](evs: immutable.Seq[Ev]): ProcessingResult[Ev] =
        new ProcessingResult(evs.toVector, Vector.empty, Vector.empty)

      /**
        * Creates a result with the specified before update side effect and no events.
        *
        * @param sideEffect the side effect to perform 
        * @return processing result
        */
      def withBeforeUpdateSideEffect[Ev](sideEffect: () => Future[_]): ProcessingResult[Ev] =
        new ProcessingResult(Vector.empty, Vector(sideEffect), Vector.empty)

      /**
        * Creates a result with the specified after update side effect and no events.
        *
        * @param sideEffect the side effect to perform
        * @return processing result
        */
      def withAfterUpdateSideEffect[Ev](sideEffect: () => Future[_]): ProcessingResult[Ev] =
        new ProcessingResult(Vector.empty, Vector.empty, Vector(sideEffect))
    }

    /**
      * Builds an [[EventBased]] stateful flow logic with no support for commands.
      *
      * @param initialStateF the initial state used when no previous state can be recovered
      * @param processInputF the processing function to be used for flow inputs
      * @param updateStateF the state update function used to update the state with the events generated by the processing
      * @return An instance of [[EventBased]] stateful flow logic
      * @tparam State the state type managed by this logic
      * @tparam Ev the type of events generated by this logic when processing inputs
      * @tparam In the type of inputs handled by this logic
      */
    def apply[State, Ev, In](
        initialStateF: () => State,
        processInputF: (State, In) => ProcessingResult[Ev],
        updateStateF: (State, Ev) => State
      ): EventBased[State, Ev, In, NoCommand] = {
      new EventBased[State, Ev, In, NoCommand] {
        override def initialState: State = initialStateF()

        override def processCommand(state: State, command: NoCommand): ProcessingResult[Ev] =
          throw new IllegalStateException(
            "Inconsistency in StatefulFlowLogic.EventBased! Received unexpected command"
          )

        override def processInput(state: State, in: In): ProcessingResult[Ev] =
          processInputF(state, in)

        override def updateState(state: State, event: Ev): State = updateStateF(state, event)
      }
    }

    /**
      * Builds an [[EventBased]] stateful flow logic with support for commands.
      *
      * @param initialStateF the initial state used when no previous state can be recovered
      * @param processInputF the processing function to be used for flow inputs
      * @param updateStateF the state update function used to update the state with the events generated by the processing
      * @param processCommandF the processing function to be used for commands
      * @return An instance of [[EventBased]] stateful flow logic
      * @tparam State the state type managed by this logic
      * @tparam Ev the type of events generated by this logic when processing inputs/commands
      * @tparam In the type of inputs handled by this logic
      * @tparam Command the type of commands handled by this logic
      */
    def apply[State, Ev, In, Command](
        initialStateF: () => State,
        processInputF: (State, In) => ProcessingResult[Ev],
        updateStateF: (State, Ev) => State,
        processCommandF: (State, Command) => ProcessingResult[Ev]
      ): EventBased[State, Ev, In, Command] = {
      new EventBased[State, Ev, In, Command] {
        override def initialState: State = initialStateF()

        override def processCommand(state: State, command: Command): ProcessingResult[Ev] =
          processCommandF(state, command)

        override def processInput(state: State, in: In): ProcessingResult[Ev] =
          processInputF(state, in)

        override def updateState(state: State, event: Ev): State = updateStateF(state, event)
      }
    }
  }

  /**
    * A [[StatefulFlowLogic]] where the state is modified in place.
    *
    * This kind of logic is well suited to implement standard CRUD services. 
    */
  trait DurableState[State, In, Out, Command] extends StatefulFlowLogic[State, In, Out, Command] {
    private[spekka] def processCommand(
        state: State,
        command: Command
      ): DurableState.ProcessingResult[State, Nothing]
    private[spekka] def processInput(
        state: State,
        in: In
      ): DurableState.ProcessingResult[State, Out]

    /**
      * Creates a [[StatefulFlowProps]] for this logic with the specified backend.
      *
      * @param backend An [[StatefulFlowBackend.DurableState]] backend to use together with this logic to create a stateful flow
      * @return [[StatefulFlowProps]] for this logic and the specified backend
      */
    def propsForBackend[Protocol](
        backend: StatefulFlowBackend.DurableState[State, Protocol]
      ): StatefulFlowProps[In, Out, Command] = backend.propsForLogic(this)
  }

  /**
    * Namespace object for [[EventBased]] stateful flow logics.
    */
  object DurableState {

    /**
      * Represent the result of processing an input/command by a [[DurableState]] stateful flow logic.
      *
      * The result must contain the new state and may specify a series of side-effects to be performed
      * before and after the state is updated and a series of outputs to be produced by the flow.
      *
      * Is is guaranteed that all `beforeUpdateSideEffects` will be completed successfully before the state
      * is updated with the new one. Furthermore it is guaranteed that `afterUpdateSideEffects` will be executed
      * only after the state has been successfully updated.
      *
      * In case the side effects or the state update fail, the flow associated to this logic will fail.
      */
    class ProcessingResult[State, Out] private[ProcessingResult] (
        private[spekka] val state: State,
        private[spekka] val outs: Vector[Out],
        private[spekka] val beforeUpdateSideEffects: Vector[() => Future[_]],
        private[spekka] val afterUpdateSideEffects: Vector[() => Future[_]]) {

      /**
        * Checks whether this result has side effects
        *
        * @return true if there are side effect in this result
        */
      def hasSideEffects: Boolean =
        beforeUpdateSideEffects.nonEmpty || afterUpdateSideEffects.nonEmpty

      /**
        * Transform the state in this result by applying the provided function.
        *
        * @param f the state transformation function
        * @return updated result
        */
      def updateState(f: State => State): ProcessingResult[State, Out] =
        new ProcessingResult(f(state), outs, beforeUpdateSideEffects, afterUpdateSideEffects)

      /**
        * Append the specified output to the list of outputs in this result.
        *
        * @param out the output to append
        * @return updated result
        */
      def withOutput(out: Out): ProcessingResult[State, Out] =
        new ProcessingResult(state, outs :+ out, beforeUpdateSideEffects, afterUpdateSideEffects)

      /**
        * Append all the specified outputs to the list of outputs in this result.
        *
        * @param os the outputs to append
        * @return updated result
        */
      def withOutputs(os: immutable.Seq[Out]): ProcessingResult[State, Out] =
        new ProcessingResult(state, outs ++ os, beforeUpdateSideEffects, afterUpdateSideEffects)


      /**
        * Append a side effect to the list of side effects to perform before updating the state.
        *
        * @param sideEffect the side effect to append
        * @return updated result
        */
      def withBeforeUpdateSideEffect(sideEffect: () => Future[_]): ProcessingResult[State, Out] =
        new ProcessingResult(
          state,
          outs,
          beforeUpdateSideEffects :+ sideEffect,
          afterUpdateSideEffects
        )

      /**
        * Append all the side effects to the list of side effects to perform before updating the state.
        *
        * @param sideEffects the side effects to append
        * @return updated result
        */
      def withBeforeUpdateSideEffects(
          sideEffects: immutable.Iterable[() => Future[_]]
        ): ProcessingResult[State, Out] =
        new ProcessingResult(
          state,
          outs,
          beforeUpdateSideEffects ++ sideEffects,
          afterUpdateSideEffects
        )

      /**
        * Append a side effect to the list of side effects to perform after the state has been updated.
        *
        * @param sideEffect the side effect to append
        * @return updated result
        */
      def withAfterUpdateSideEffect(sideEffect: () => Future[_]): ProcessingResult[State, Out] =
        new ProcessingResult(
          state,
          outs,
          beforeUpdateSideEffects,
          afterUpdateSideEffects :+ sideEffect
        )

      /**
        * Append all the side effects to the list of side effects to perform after the state has been updated.
        *
        * @param sideEffects the side effects to append
        * @return updated result
        */
      def withAfterUpdateSideEffects(
          sideEffects: immutable.Iterable[() => Future[_]]
        ): ProcessingResult[State, Out] =
        new ProcessingResult(
          state,
          outs,
          beforeUpdateSideEffects,
          afterUpdateSideEffects ++ sideEffects
        )
    }

    /**
      * Processing result builder
      */
    object ProcessingResult {
      /**
        * Create a result with the specified state and outputs.
        *
        * @param state the new state
        * @param outs the outputs to produce
        * @return processing result
        */
      def apply[State, Out](state: State, outs: Out*): ProcessingResult[State, Out] =
        new ProcessingResult(state, outs.toVector, Vector.empty, Vector.empty)
    }

    /**
      * Builds a [[DurableState]] stateful flow logic with no support for commands.
      *
      * @param initialStateF the initial state used when no previous state can be recovered
      * @param processInputF the processing function to be used for flow inputs
      * @return An instance of [[DurableState]] stateful flow logic
      * @tparam State the state type managed by this logic
      * @tparam In the type of inputs handled by this logic
      * @tparam Out the type of outputs produced by this logic
      */
    def apply[State, In, Out](
        initialStateF: () => State,
        processInputF: (State, In) => ProcessingResult[State, Out]
      ): DurableState[State, In, Out, NoCommand] = {
      new DurableState[State, In, Out, NoCommand] {
        override def initialState: State = initialStateF()

        override def processCommand(
            state: State,
            command: NoCommand
          ): ProcessingResult[State, Nothing] =
          throw new IllegalStateException(
            "Inconsistency in StatefulFlowLogic.DurableState! Received unexpected command"
          )

        override def processInput(state: State, in: In): ProcessingResult[State, Out] =
          processInputF(state, in)
      }
    }

    /**
      * Builds a [[DurableState]] stateful flow logic with support for commands.
      *
      * @param initialStateF the initial state used when no previous state can be recovered
      * @param processInputF the processing function to be used for flow inputs
      * @param processCommandF the processing function to be used for commands
      * @return An instance of [[DurableState]] stateful flow logic
      * @tparam State the state type managed by this logic
      * @tparam In the type of inputs handled by this logic
      * @tparam Out the type of outputs produced by this logic
      * @tparam Command the type of commands handled by this logic
      */
    def apply[State, In, Out, Command](
        initialStateF: () => State,
        processInputF: (State, In) => ProcessingResult[State, Out],
        processCommandF: (State, Command) => ProcessingResult[State, Nothing]
      ): DurableState[State, In, Out, Command] = {
      new DurableState[State, In, Out, Command] {
        override def initialState: State = initialStateF()

        override def processCommand(
            state: State,
            command: Command
          ): ProcessingResult[State, Nothing] = processCommandF(state, command)

        override def processInput(state: State, in: In): ProcessingResult[State, Out] =
          processInputF(state, in)
      }
    }
  }
}
