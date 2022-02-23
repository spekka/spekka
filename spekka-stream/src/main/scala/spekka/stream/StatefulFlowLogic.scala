package spekka.stream

import scala.collection.immutable
import scala.concurrent.Future

sealed trait StatefulFlowLogic[State, +In, -Out, Command] {
  def initialState: State
}

object StatefulFlowLogic {
  class ProcessingResult[Ev] private[ProcessingResult](
    val events: Vector[Ev],
    val beforeUpdateSideEffects: Vector[() => Future[_]],
    val afterUpdateSideEffects: Vector[() => Future[_]]
  ) {
    def withEvent(ev: Ev): ProcessingResult[Ev] =
      new ProcessingResult(events :+ ev, beforeUpdateSideEffects, afterUpdateSideEffects)

    def withEvents(ev: immutable.Seq[Ev]): ProcessingResult[Ev] =
      new ProcessingResult(events :++ ev, beforeUpdateSideEffects, afterUpdateSideEffects)

    def withBeforeUpdateSideEffect(sideEffect: () => Future[_]): ProcessingResult[Ev] =
      new ProcessingResult(events, beforeUpdateSideEffects :+ sideEffect, afterUpdateSideEffects)

    def withAfterUpdateSideEffect(sideEffect: () => Future[_]): ProcessingResult[Ev] =
      new ProcessingResult(events, beforeUpdateSideEffects, afterUpdateSideEffects :+ sideEffect)

    def combine(other: ProcessingResult[Ev]): ProcessingResult[Ev] =
      new ProcessingResult(
        events ++ other.events,
        beforeUpdateSideEffects ++ other.beforeUpdateSideEffects,
        afterUpdateSideEffects ++ other.afterUpdateSideEffects
      )
  }

  object ProcessingResult {
    def empty[Ev]: ProcessingResult[Ev] = new ProcessingResult(Vector.empty, Vector.empty, Vector.empty)

    def withEvent[Ev](ev: Ev): ProcessingResult[Ev] = new ProcessingResult(Vector(ev), Vector.empty, Vector.empty)

    def withEvents[Ev](evs: immutable.Seq[Ev]): ProcessingResult[Ev] =
      new ProcessingResult(evs.toVector, Vector.empty, Vector.empty)

    def withBeforeUpdateSideEffect[Ev](sideEffect: () => Future[_]): ProcessingResult[Ev] =
      new ProcessingResult(Vector.empty, Vector(sideEffect), Vector.empty)

    def withAfterUpdateSideEffect[Ev](sideEffect: () => Future[_]): ProcessingResult[Ev] =
      new ProcessingResult(Vector.empty, Vector.empty, Vector(sideEffect))
  }
  


  trait EventBased[State, Ev, In, Command] extends StatefulFlowLogic[State, In, Ev, Command] {
    def processCommand(command: Command): ProcessingResult[Ev]
    def processInput(in: In): ProcessingResult[Ev]
    def updateState(state: State, event: Ev): State
  }

  sealed trait NoCommand

  def eventBased[State, Ev, In](
    initialStateF: () => State,
    processInputF: In => ProcessingResult[Ev],
    updateStateF: (State, Ev) => State
  ): EventBased[State, Ev, In, NoCommand] = {
    new EventBased[State, Ev, In, NoCommand] {
      override def initialState: State = initialStateF()

      override def processCommand(command: NoCommand): ProcessingResult[Ev] =
        throw new IllegalStateException("Inconsistency in StatefulFlowLogic.eventBased! Received unexpected command")

      override def processInput(in: In): ProcessingResult[Ev] = processInputF(in)

      override def updateState(state: State, event: Ev): State = updateStateF(state, event) 
    }
  }

  def eventBased[State, Ev, In, Command](
      initialStateF: () => State,
      processInputF: In => ProcessingResult[Ev],
      updateStateF: (State, Ev) => State,
      processCommandF: Command => ProcessingResult[Ev]
    ): EventBased[State, Ev, In, Command] = {
      new EventBased[State, Ev, In, Command] {
        override def initialState: State = initialStateF()

        override def processCommand(command: Command): ProcessingResult[Ev] =
          processCommandF(command)

        override def processInput(in: In): ProcessingResult[Ev] = processInputF(in)

        override def updateState(state: State, event: Ev): State = updateStateF(state, event) 
      }
    }
}
