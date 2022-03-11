package spekka.stateful

import akka.Done
import akka.actor.typed.ActorRef
import akka.pattern.StatusReply

private[spekka] object StatefulFlowHandler {
  sealed trait Protocol[+In, -Out, +Command, -B]

  case class ProcessFlowInput[In, Out, Pass](
      in: In,
      passthrough: Pass,
      replyTo: ActorRef[StatusReply[(Seq[Out], Pass)]])
      extends Protocol[In, Out, Nothing, Any]

  case class ProcessCommand[Command](
      command: Command)
      extends Protocol[Nothing, Any, Command, Any]

  case class TerminateRequest(replyTo: ActorRef[StatusReply[Done]])
      extends Protocol[Nothing, Any, Nothing, Any]

  trait BackendProtocol[B] extends Protocol[Nothing, Any, Nothing, B]

}
