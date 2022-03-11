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

import scala.concurrent.Future

trait StatefulFlowBuilder[In, Out, Command] {
  val entityKind: String

  def flow(entityId: String): Flow[In, Seq[Out], Future[StatefulFlowControl[Command]]]

  def flowWithContext[Ctx](
      entityId: String
    ): FlowWithContext[In, Ctx, Seq[Out], Ctx, Future[StatefulFlowControl[Command]]]

  def control(entityId: String): Future[Option[StatefulFlowControl[Command]]]
}
