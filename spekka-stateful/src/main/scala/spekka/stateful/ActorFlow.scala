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

import akka.NotUsed
import akka.actor.typed.ActorRef
import akka.pattern.AskTimeoutException
import akka.pattern.StatusReply
import akka.stream.WatchedActorTerminatedException
import akka.stream.scaladsl.Flow
import akka.util.Timeout

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

/** Implements a context aware variant of Akka's ActorFlow.ask.
  *
  * The rationale for this implementation avoid serialization hassle when working with remote
  * actors. Indeed the context of the flow may be not known (making it difficult to configure a
  * serializer) or not serializable at all.
  *
  * The code is taken straight from Akka's ActorFlow.scala with minor adaptations.
  *
  * NOTE: This should be removed if this is resolved: https://github.com/akka/akka/issues/31308
  */
private[spekka] object ActorFlow {

  def askWithContext[I, Q, A, Ctx](
      ref: ActorRef[Q]
    )(makeMessage: (I, ActorRef[A]) => Q
    )(implicit
      timeout: Timeout,
      ec: ExecutionContext
    ): Flow[(I, Ctx), (A, Ctx), NotUsed] =
    askWithContext[I, Q, A, Ctx](parallelism = 2)(ref)(makeMessage)(timeout, ec)

  def askWithContext[I, Q, A, Ctx](
      parallelism: Int
    )(ref: ActorRef[Q]
    )(makeMessage: (I, ActorRef[A]) => Q
    )(implicit
      timeout: Timeout,
      ec: ExecutionContext
    ): Flow[(I, Ctx), (A, Ctx), NotUsed] = {
    import akka.actor.typed.scaladsl.adapter._
    val classicRef = ref.toClassic

    val askFlow = Flow[(I, Ctx)]
      .watch(classicRef)
      .mapAsync(parallelism) { case (el, ctx) =>
        val res = akka.pattern.extended.ask(
          classicRef,
          (replyTo: akka.actor.ActorRef) => makeMessage(el, replyTo)
        )
        // we need to cast manually (yet safely, by construction!) since otherwise we need a ClassTag,
        // which in Scala is fine, but then we would force JavaDSL to create one, which is a hassle in the Akka Typed DSL,
        // since one may say "but I already specified the type!", and that we have to go via the classic ask is an implementation detail
        res.asInstanceOf[Future[A]].map(_ -> ctx)
      }
      .mapError {
        case ex: AskTimeoutException =>
          // in Akka Typed we use the `TimeoutException` everywhere
          new java.util.concurrent.TimeoutException(ex.getMessage)

        // the purpose of this recovery is to change the name of the stage in that exception
        // we do so in order to help users find which stage caused the failure -- "the ask stage"
        case ex: WatchedActorTerminatedException =>
          new WatchedActorTerminatedException("ask()", ex.ref)
      }
      .named("ask")

    askFlow
  }

  def askWithStatusAndContext[I, Q, A, Ctx](
      ref: ActorRef[Q]
    )(makeMessage: (I, ActorRef[StatusReply[A]]) => Q
    )(implicit
      timeout: Timeout,
      ec: ExecutionContext
    ): Flow[(I, Ctx), (A, Ctx), NotUsed] = askWithStatusAndContext(2)(ref)(makeMessage)

  def askWithStatusAndContext[I, Q, A, Ctx](
      parallelism: Int
    )(ref: ActorRef[Q]
    )(makeMessage: (I, ActorRef[StatusReply[A]]) => Q
    )(implicit
      timeout: Timeout,
      ec: ExecutionContext
    ): Flow[(I, Ctx), (A, Ctx), NotUsed] = {
    askWithContext[I, Q, StatusReply[A], Ctx](parallelism)(ref)(makeMessage).map {
      case (StatusReply.Success(a), ctx) => a.asInstanceOf[A] -> ctx
      case (StatusReply.Error(err), _) => throw err
      case _ => throw new RuntimeException() // compiler exhaustiveness check pleaser
    }
  }

}
