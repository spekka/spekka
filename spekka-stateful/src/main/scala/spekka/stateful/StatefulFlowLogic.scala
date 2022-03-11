package spekka.stateful

import scala.collection.immutable
import scala.concurrent.Future

sealed trait StatefulFlowLogic[State, +In, -Out, Command] {
  private[spekka] def initialState: State
}

object StatefulFlowLogic {
  sealed trait NoCommand

  trait EventBased[State, Ev, In, Command] extends StatefulFlowLogic[State, In, Ev, Command] {
    private[spekka] def processCommand(
        state: State,
        command: Command
      ): EventBased.ProcessingResult[Ev]
    private[spekka] def processInput(state: State, in: In): EventBased.ProcessingResult[Ev]
    private[spekka] def updateState(state: State, event: Ev): State

    def propsForBackend[Protocol](
        backend: StatefulFlowBackend.EventBased[State, Ev, Protocol]
      ): StatefulFlowProps[In, Ev, Command] = backend.propsForLogic(this)
  }

  object EventBased {
    class ProcessingResult[Ev] private[ProcessingResult] (
        val events: Vector[Ev],
        val beforeUpdateSideEffects: Vector[() => Future[_]],
        val afterUpdateSideEffects: Vector[() => Future[_]]) {

      def hasSideEffects: Boolean =
        beforeUpdateSideEffects.nonEmpty || afterUpdateSideEffects.nonEmpty

      def withEvent(ev: Ev): ProcessingResult[Ev] =
        new ProcessingResult(events :+ ev, beforeUpdateSideEffects, afterUpdateSideEffects)

      def withEvents(ev: immutable.Seq[Ev]): ProcessingResult[Ev] =
        new ProcessingResult(events ++ ev, beforeUpdateSideEffects, afterUpdateSideEffects)

      def withBeforeUpdateSideEffect(sideEffect: () => Future[_]): ProcessingResult[Ev] =
        new ProcessingResult(events, beforeUpdateSideEffects :+ sideEffect, afterUpdateSideEffects)

      def withBeforeUpdateSideEffects(
          sideEffects: immutable.Iterable[() => Future[_]]
        ): ProcessingResult[Ev] =
        new ProcessingResult(events, beforeUpdateSideEffects ++ sideEffects, afterUpdateSideEffects)

      def withAfterUpdateSideEffect(sideEffect: () => Future[_]): ProcessingResult[Ev] =
        new ProcessingResult(events, beforeUpdateSideEffects, afterUpdateSideEffects :+ sideEffect)

      def withAfterUpdateSideEffects(
          sideEffects: immutable.Iterable[() => Future[_]]
        ): ProcessingResult[Ev] =
        new ProcessingResult(events, beforeUpdateSideEffects, afterUpdateSideEffects ++ sideEffects)

      def combine(other: ProcessingResult[Ev]): ProcessingResult[Ev] =
        new ProcessingResult(
          events ++ other.events,
          beforeUpdateSideEffects ++ other.beforeUpdateSideEffects,
          afterUpdateSideEffects ++ other.afterUpdateSideEffects
        )
    }

    object ProcessingResult {
      def empty[Ev]: ProcessingResult[Ev] =
        new ProcessingResult(Vector.empty, Vector.empty, Vector.empty)

      def withEvent[Ev](ev: Ev): ProcessingResult[Ev] =
        new ProcessingResult(Vector(ev), Vector.empty, Vector.empty)

      def withEvents[Ev](evs: immutable.Seq[Ev]): ProcessingResult[Ev] =
        new ProcessingResult(evs.toVector, Vector.empty, Vector.empty)

      def withBeforeUpdateSideEffect[Ev](sideEffect: () => Future[_]): ProcessingResult[Ev] =
        new ProcessingResult(Vector.empty, Vector(sideEffect), Vector.empty)

      def withAfterUpdateSideEffect[Ev](sideEffect: () => Future[_]): ProcessingResult[Ev] =
        new ProcessingResult(Vector.empty, Vector.empty, Vector(sideEffect))
    }

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

  trait DurableState[State, In, Out, Command] extends StatefulFlowLogic[State, In, Out, Command] {
    private[spekka] def processCommand(
        state: State,
        command: Command
      ): DurableState.ProcessingResult[State, Nothing]
    private[spekka] def processInput(
        state: State,
        in: In
      ): DurableState.ProcessingResult[State, Out]

    def propsForBackend[Protocol](
        backend: StatefulFlowBackend.DurableState[State, Protocol]
      ): StatefulFlowProps[In, Out, Command] = backend.propsForLogic(this)
  }

  object DurableState {
    class ProcessingResult[State, Out] private[ProcessingResult] (
        val state: State,
        val outs: Vector[Out],
        val beforeUpdateSideEffects: Vector[() => Future[_]],
        val afterUpdateSideEffects: Vector[() => Future[_]]) {

      def hasSideEffects: Boolean =
        beforeUpdateSideEffects.nonEmpty || afterUpdateSideEffects.nonEmpty

      def withState(state: State): ProcessingResult[State, Out] =
        new ProcessingResult(state, outs, beforeUpdateSideEffects, afterUpdateSideEffects)

      def updateState(f: State => State): ProcessingResult[State, Out] =
        new ProcessingResult(f(state), outs, beforeUpdateSideEffects, afterUpdateSideEffects)

      private[spekka] def mapState[State2](f: State => State2): ProcessingResult[State2, Out] =
        new ProcessingResult(f(state), outs, beforeUpdateSideEffects, afterUpdateSideEffects)

      def withOutput(out: Out): ProcessingResult[State, Out] =
        new ProcessingResult(state, outs :+ out, beforeUpdateSideEffects, afterUpdateSideEffects)

      def withOutputs(os: immutable.Seq[Out]): ProcessingResult[State, Out] =
        new ProcessingResult(state, outs ++ os, beforeUpdateSideEffects, afterUpdateSideEffects)

      def withBeforeUpdateSideEffect(sideEffect: () => Future[_]): ProcessingResult[State, Out] =
        new ProcessingResult(
          state,
          outs,
          beforeUpdateSideEffects :+ sideEffect,
          afterUpdateSideEffects
        )

      def withBeforeUpdateSideEffects(sideEffects: immutable.Iterable[() => Future[_]]): ProcessingResult[State, Out] =
        new ProcessingResult(
          state,
          outs,
          beforeUpdateSideEffects ++ sideEffects,
          afterUpdateSideEffects
        )

      def withAfterUpdateSideEffect(sideEffect: () => Future[_]): ProcessingResult[State, Out] =
        new ProcessingResult(
          state,
          outs,
          beforeUpdateSideEffects,
          afterUpdateSideEffects :+ sideEffect
        )

      def withAfterUpdateSideEffects(sideEffects: immutable.Iterable[() => Future[_]]): ProcessingResult[State, Out] =
        new ProcessingResult(
          state,
          outs,
          beforeUpdateSideEffects,
          afterUpdateSideEffects ++ sideEffects
        )
    }

    object ProcessingResult {
      def apply[State, Out](state: State, outs: Out*): ProcessingResult[State, Out] =
        new ProcessingResult(state, outs.toVector, Vector.empty, Vector.empty)
    }

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
