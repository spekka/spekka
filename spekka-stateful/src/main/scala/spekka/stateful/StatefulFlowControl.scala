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

import scala.concurrent.Future

/**
  * Control interface for a stateful flow.
  *
  * It allows to issue command (and receive responses) to a particular stateful flow instance as
  * well as request its termination.
  *
  * @tparam Command the type of commands supported by the stateful flow
  */
trait StatefulFlowControl[Command] {

  /**
    * Issue a command in a fire-and-forget manner to the stateful flow instance.
    *
    * @param command the command to issue
    */
  def command(command: Command): Unit

  /**
    * Issue a command to the stateful flow instance and expect a reply.
    *
    * The way commands with reply are modeled is analogous to Akka Typed: 
    * you must add a field of type `ActorRef[StatusReply[Result]]` in your command class and
    * use it in you logic to send the reply.
    *
    * The actual actor ref to use is handled by spekka and provided to you as an argument to the builder function `f`.
    *
    * {{{
    *    case class GetCounter(replyTo: ActorRef[StatusReply[Long]])
    *    //...
    *    val control: StatefulFlowControl[GetCounter] = ???
    *
    *    val counterFuture: Future[Long] = control.commandWithResult(replyTo => GetCounter(replyTo))
    * }}}
    *
    * @param f the command builder function used to inject the reply actor ref into the command
    * @return a future of the result
    */
  def commandWithResult[Result](f: ActorRef[StatusReply[Result]] => Command): Future[Result]

  /**
    * Request the termination of the stateful flow instance
    *
    * @return a future completing when the stateful flow is terminated
    */
  def terminate(): Future[Done]
}
