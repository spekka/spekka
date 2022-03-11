package spekka.stateful

import akka.actor.typed.Behavior

sealed trait StatefulFlowProps[In, Out, Command] {
  type BP
  def backend: StatefulFlowBackend
  def behaviorFor(
      entityKind: String,
      entityId: String
    ): Behavior[StatefulFlowHandler.Protocol[In, Out, Command, BP]]
}

private[spekka] object StatefulFlowProps {
  class EventBased[State, Ev, In, Command, BackendProtocol](
      logic: StatefulFlowLogic.EventBased[State, Ev, In, Command],
      val backend: StatefulFlowBackend.EventBased[State, Ev, BackendProtocol])
      extends StatefulFlowProps[In, Ev, Command] {

    type BP = BackendProtocol

    override def behaviorFor(
        entityKind: String,
        entityId: String
      ): Behavior[StatefulFlowHandler.Protocol[In, Ev, Command, BackendProtocol]] =
      backend.behaviorFor(logic, entityKind, entityId)
  }

  class DurableState[State, In, Out, Command, BackendProtocol](
      logic: StatefulFlowLogic.DurableState[State, In, Out, Command],
      val backend: StatefulFlowBackend.DurableState[State, BackendProtocol])
      extends StatefulFlowProps[In, Out, Command] {

    type BP = BackendProtocol
    override def behaviorFor(
        entityKind: String,
        entityId: String
      ): Behavior[StatefulFlowHandler.Protocol[In, Out, Command, BackendProtocol]] =
      backend.behaviorFor(logic, entityKind, entityId)
  }
}
