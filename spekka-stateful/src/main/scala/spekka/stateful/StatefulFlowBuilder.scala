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

import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.FlowWithContext
import spekka.context.FlowWithExtendedContext

import scala.concurrent.Future

/** A [[StatefulFlowBuilder]] can be used to create instances of a registered stateful flow.
  *
  * Once a stateful flow (in the form of a [[StatefulFlowProps]]) is registered on a
  * [[StatefulFlowRegistry]], the returned [[StatefulFlowBuilder]] object can be used to create
  * instances of the flow for different entities.
  *
  * Each entity will be backed by a different instance of logic/backend and their lifecycle will be
  * completely disjointed.
  *
  * A flow obtained by a [[StatefulFlowBuilder]] can be materialized multiple times, however all the
  * materializations will be backed by the same logic/backend instance. Similarly requesting again a
  * flow for the same entity will produce a flow backed by the same logic/backend instance.
  *
  * @tparam In
  *   the input type of the stateful flow
  * @tparam Out
  *   the output type of the stateful flow
  * @tparam Command
  *   the command type of the stateful flow
  */
trait StatefulFlowBuilder[In, Out, Command] {

  /** The entity kind specified when the stateful flow was registered on the
    * [[StatefulFlowRegistry]]
    */
  val entityKind: String

  /** Returns a flow for the specified entity.
    *
    * @param entityId
    *   the entity to create the flow for
    * @return
    *   a stateful flow instance for the specified entity
    */
  def flow(entityId: String): Flow[In, Seq[Out], Future[StatefulFlowControl[Command]]]

  /** Return a flow with context for the specified entity.
    *
    * @param entityId
    *   the entity to create the flow for
    * @return
    *   a stateful flow instance for the specified entity
    */
  def flowWithContext[Ctx](
      entityId: String
    ): FlowWithContext[In, Ctx, Seq[Out], Ctx, Future[StatefulFlowControl[Command]]]

  /** Return a flow with extended context for the specified entity.
    *
    * @param entityId
    *   the entity to create the flow for
    * @return
    *   a stateful flow instance for the specified entity
    */
  def flowWithExtendedContext[Ctx](
      entityId: String
    ): FlowWithExtendedContext[In, Seq[Out], Ctx, Future[StatefulFlowControl[Command]]]

  /** Return the control interface for the current stateful flow materialization of the specified
    * entity.
    *
    * @param entityId
    *   the entity to fetch the control interface of
    * @return
    *   the control interface or None if the entity is not materialized
    */
  def control(entityId: String): Future[Option[StatefulFlowControl[Command]]]
}
