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
import scala.util.control.NoStackTrace

/** Control interface for a stateful flow.
  *
  * It allows to issue command (and receive responses) to a particular stateful flow instance as
  * well as request its termination.
  *
  * @tparam Command
  *   the type of commands supported by the stateful flow
  */
trait StatefulFlowControl[Command] {

  /** Issue a command in a fire-and-forget manner to the stateful flow instance.
    *
    * @param command
    *   the command to issue
    */
  def command(command: Command): Unit

  /** Issue a command to the stateful flow instance and expect a reply.
    *
    * The way commands with reply are modeled is analogous to Akka Typed: you must add a field of
    * type `ActorRef[StatusReply[Result]]` in your command class and use it in you logic to send the
    * reply.
    *
    * The actual actor ref to use is handled by spekka and provided to you as an argument to the
    * builder function `f`.
    *
    * {{{
    *     case class GetCounter(replyTo: ActorRef[StatusReply[Long]])
    *     //...
    *     val control: StatefulFlowControl[GetCounter] = ???
    *
    *     val counterFuture: Future[Long] = control.commandWithResult(replyTo => GetCounter(replyTo))
    * }}}
    *
    * @param f
    *   the command builder function used to inject the reply actor ref into the command
    * @return
    *   a future of the result
    */
  def commandWithResult[Result](f: ActorRef[StatusReply[Result]] => Command): Future[Result]

  /** Request the termination of the stateful flow instance
    *
    * @return
    *   a future completing when the stateful flow is terminated
    */
  def terminate(): Future[Done]
}

case class StatefulFlowNotMaterialized(entityKind: String, entityId: String)
    extends Exception(s"Entity with kind [${entityKind}] and id [${entityId}] is not materialized")
    with NoStackTrace

/** Control interface for a stateful flow.
  *
  * It allows to issue command (and receive responses) to the currently materialized stateful flow
  * instance for the backing entity.
  *
  * @tparam Command
  *   the type of commands supported by the stateful flow
  */
trait StatefulFlowLazyEntityControl[Command] {

  /** Issue a command in a fire-and-forget manner to the stateful flow instance.
    *
    * In case there are no currently materialized statful flow for the backing entity, the future
    * will fail with a [[StatefulFlowNotMaterialized]] exception.
    *
    * @param command
    *   the command to issue
    * @return
    *   a future completing when the command is actually sent
    */
  def command(command: Command): Future[Unit]

  /** Issue a command in a fire-and-forget manner to the stateful flow instance.
    *
    * In case there is no currently materialized statful flow for the backing entity, the future
    * will succede with a false value.
    *
    * @param command
    *   the command to issue
    * @return
    *   a future indicating whether the command has been actually sent
    */
  def commandOption(command: Command): Future[Boolean]

  /** Issue a command to the stateful flow instance and expect a reply.
    *
    * The way commands with reply are modeled is analogous to Akka Typed: you must add a field of
    * type `ActorRef[StatusReply[Result]]` in your command class and use it in you logic to send the
    * reply.
    *
    * The actual actor ref to use is handled by spekka and provided to you as an argument to the
    * builder function `f`.
    *
    * {{{
    *     case class GetCounter(replyTo: ActorRef[StatusReply[Long]])
    *     //...
    *     val control: StatefulFlowControl[GetCounter] = ???
    *
    *     val counterFuture: Future[Long] = control.commandWithResult(replyTo => GetCounter(replyTo))
    * }}}
    *
    * In case there are no currently materialized statful flow for the backing entity, the future
    * will fail with a [[StatefulFlowNotMaterialized]] exception.
    *
    * @param f
    *   the command builder function used to inject the reply actor ref into the command
    * @return
    *   a future of the result
    */
  def commandWithResult[Result](f: ActorRef[StatusReply[Result]] => Command): Future[Result]

  /** Issue a command to the stateful flow instance and expect a reply.
    *
    * The way commands with reply are modeled is analogous to Akka Typed: you must add a field of
    * type `ActorRef[StatusReply[Result]]` in your command class and use it in you logic to send the
    * reply.
    *
    * The actual actor ref to use is handled by spekka and provided to you as an argument to the
    * builder function `f`.
    *
    * {{{
    *     case class GetCounter(replyTo: ActorRef[StatusReply[Long]])
    *     //...
    *     val control: StatefulFlowControl[GetCounter] = ???
    *
    *     val counterFuture: Future[Long] = control.commandWithResult(replyTo => GetCounter(replyTo))
    * }}}
    *
    * In case there are no currently materialized statful flow for the backing entity, the future
    * will succede with None.
    *
    * @param f
    *   the command builder function used to inject the reply actor ref into the command
    * @return
    *   a future of the result or None
    */
  def commandWithResultOption[Result](
      f: ActorRef[StatusReply[Result]] => Command
    ): Future[Option[Result]]
}

/** Control interface for a stateful flow.
  *
  * It allows to issue command (and receive responses) to all currently materialized stateful flows
  * instance for the backing entity kind.
  *
  * @tparam Command
  *   the type of commands supported by the stateful flow
  */
trait StatefulFlowLazyControl[Command] {

  /** Issue a command in a fire-and-forget manner to the stateful flow instance.
    *
    * In case there is no currently materialized statful flow for the backing entity, the future
    * will fail with a [[StatefulFlowNotMaterialized]] exception.
    *
    * @param entityId
    *   the entity to issue to command to
    * @param command
    *   the command to issue
    * @return
    *   a future indicating whether the command has been actually sent
    */
  def command(entityId: String, command: Command): Future[Unit]

  /** Issue a command in a fire-and-forget manner to the stateful flow instance.
    *
    * In case there is no currently materialized statful flow for the backing entity, the future
    * will succede with a false value.
    *
    * @param entityId
    *   the entity to issue to command to
    * @param command
    *   the command to issue
    * @return
    *   a future indicating whether the command has been actually sent
    */
  def commandOption(entityId: String, command: Command): Future[Boolean]

  /** Issue a command to the stateful flow instance and expect a reply.
    *
    * The way commands with reply are modeled is analogous to Akka Typed: you must add a field of
    * type `ActorRef[StatusReply[Result]]` in your command class and use it in you logic to send the
    * reply.
    *
    * The actual actor ref to use is handled by spekka and provided to you as an argument to the
    * builder function `f`.
    *
    * {{{
    *     case class GetCounter(replyTo: ActorRef[StatusReply[Long]])
    *     //...
    *     val control: StatefulFlowControl[GetCounter] = ???
    *
    *     val counterFuture: Future[Long] = control.commandWithResult(replyTo => GetCounter(replyTo))
    * }}}
    *
    * In case there are no currently materialized statful flow for the specified entity, the future
    * will fail with a [[StatefulFlowNotMaterialized]] exception.
    *
    * @param entityId
    *   the entity to issue to command to
    * @param f
    *   the command builder function used to inject the reply actor ref into the command
    * @return
    *   a future of the result
    */
  def commandWithResult[Result](
      entityId: String,
      f: ActorRef[StatusReply[Result]] => Command
    ): Future[Result]

  /** Issue a command to the stateful flow instance and expect a reply.
    *
    * The way commands with reply are modeled is analogous to Akka Typed: you must add a field of
    * type `ActorRef[StatusReply[Result]]` in your command class and use it in you logic to send the
    * reply.
    *
    * The actual actor ref to use is handled by spekka and provided to you as an argument to the
    * builder function `f`.
    *
    * {{{
    *     case class GetCounter(replyTo: ActorRef[StatusReply[Long]])
    *     //...
    *     val control: StatefulFlowControl[GetCounter] = ???
    *
    *     val counterFuture: Future[Long] = control.commandWithResult(replyTo => GetCounter(replyTo))
    * }}}
    *
    * In case there are no currently materialized statful flow for the specified entity, the future
    * will succede with None.
    *
    * @param entityId
    *   the entity to issue to command to
    * @param f
    *   the command builder function used to inject the reply actor ref into the command
    * @return
    *   a future of the result or None
    */
  def commandWithResultOption[Result](
      entityId: String,
      f: ActorRef[StatusReply[Result]] => Command
    ): Future[Option[Result]]

  /** Narrows this control interface to work only on the specified entity.
    *
    * @param entityId
    *   The entity to narrow the control to
    * @return
    *   An instance of [[StatefulFlowLazyEntityControl]] for the specified entity
    */
  def narrow(entityId: String): StatefulFlowLazyEntityControl[Command]
}
