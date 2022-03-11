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
