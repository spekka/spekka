package spekka.stateful

import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.pattern.StatusReply

object InMemoryStatefulFlowBackend {
  sealed private[spekka] trait InMemoryBackendProtocol
      extends StatefulFlowHandler.BackendProtocol[InMemoryBackendProtocol]

  private[spekka] case object SideEffectCompleted extends InMemoryBackendProtocol
  private[spekka] case class SideEffectFailure(ex: Throwable) extends InMemoryBackendProtocol

  implicit private[spekka] val sideEffectHandlingBehaviorProtocolAdapter
      : StatefulFlowBackend.SideEffectHandlingBehavior.ProtocolAdapter[InMemoryBackendProtocol] =
    new StatefulFlowBackend.SideEffectHandlingBehavior.ProtocolAdapter[InMemoryBackendProtocol] {
      def buildSideEffectCompleteMessage: InMemoryBackendProtocol = SideEffectCompleted

      def extractSideEffectComplete
          : PartialFunction[StatefulFlowHandler.Protocol[_, _, _, InMemoryBackendProtocol], Unit] = {
        case SideEffectCompleted =>
      }

      def buildSideEffectFailureMessage(error: Throwable): InMemoryBackendProtocol =
        SideEffectFailure(error)

      def extractSideEffectFailure
          : PartialFunction[StatefulFlowHandler.Protocol[_, _, _, InMemoryBackendProtocol], Throwable] = {
        case SideEffectFailure(ex) => ex
      }
    }

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
            case StatefulFlowHandler.ProcessFlowInput(in, pass, replyTo) =>
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
                  replyTo ! StatusReply.success(result.events -> pass)
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

    def apply[State, Ev](
        sideEffectBufferSize: Int = 128
      ): StatefulFlowBackend.EventBased[State, Ev, InMemoryBackendProtocol] =
      new StatefulFlowBackend.EventBased[State, Ev, InMemoryBackendProtocol] {
        override val id: String = "in-memory-event-based"

        override def behaviorFor[In, Command](
            logic: StatefulFlowLogic.EventBased[State, Ev, In, Command],
            entityKind: String,
            entityId: String
          ): Behavior[StatefulFlowHandler.Protocol[In, Ev, Command, InMemoryBackendProtocol]] =
          behaviorFactory(logic, sideEffectBufferSize)
      }
  }

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
            case StatefulFlowHandler.ProcessFlowInput(in, pass, replyTo) =>
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
                  replyTo ! StatusReply.success(result.outs -> pass)
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

    def apply[State](
        sideEffectBufferSize: Int = 128
      ): StatefulFlowBackend.DurableState[State, InMemoryBackendProtocol] =
      new StatefulFlowBackend.DurableState[State, InMemoryBackendProtocol] {
        override val id: String = "in-memory-durable-state"

        override def behaviorFor[In, Out, Command](
            logic: StatefulFlowLogic.DurableState[State, In, Out, Command],
            entityKind: String,
            entityId: String
          ): Behavior[StatefulFlowHandler.Protocol[In, Out, Command, InMemoryBackendProtocol]] =
          behaviorFactory(logic, sideEffectBufferSize)
      }
  }
}
