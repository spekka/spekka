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

package spekka.context

import akka.stream.scaladsl.Flow

import scala.concurrent.Future

/** Extension to [[FlowWithExtendedContext]] for Scala 2.13
  */
trait FlowWithExtendedContextVersionedExtensions {
  import FlowWithExtendedContext.syntax._

  /** Defers invoking the create function to create a future flow until there downstream demand has
    * caused upstream to send a first element.
    *
    * The materialized future value is completed with the materialized value of the created flow
    * when that has successfully been materialized.
    *
    * If the create function throws or returns a future that fails the stream is failed, in this
    * case the materialized future value is failed with a NeverMaterializedException.
    *
    * Note that asynchronous boundaries (and other operators) in the stream may do pre-fetching
    * which counter acts the laziness and can trigger the factory earlier than expected.
    */
  def lazyFutureFlow[In, Out, Ctx, M](
      create: () => Future[FlowWithExtendedContext[In, Out, Ctx, M]]
    ): FlowWithExtendedContext[In, Out, Ctx, Future[M]] = {
    implicit val ec = scala.concurrent.ExecutionContext.parasitic
    Flow.lazyFutureFlow(() => create().map(_.toFlow)).asFlowWithExtendedContextUnsafe
  }
}
