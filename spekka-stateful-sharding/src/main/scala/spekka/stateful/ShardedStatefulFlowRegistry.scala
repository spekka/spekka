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
import akka.actor.typed.Scheduler
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.sharding.typed.scaladsl.Entity
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.pattern.StatusReply
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.FlowWithContext
import akka.stream.scaladsl.Keep
import akka.util.Timeout
import spekka.context.ExtendedContext
import spekka.context.FlowWithExtendedContext

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration

/** A [[ShardedStatefulFlowRegistry]] extends a `StatefulFlowRegistry` taking care of distributing
  * the registered stateful flows on the cluster leveraging Akka Cluster Sharding.
  *
  * Flows registered on a [[ShardedStatefulFlowRegistry]] are also registered on the backing
  * `StatefulFlowRegistry`, which still handles all the actual flow materialization, however they
  * are wrapped in a sharding aware facade.
  *
  * A builder returned by a [[ShardedStatefulFlowRegistry]] is indistinguishable from one returned
  * by a `StatefulFlowRegistry`.
  *
  * It is important that stateful flows using cluster sharding are register on every node of the
  * cluster.
  */
class ShardedStatefulFlowRegistry private[spekka] (
    private[spekka] val registry: StatefulFlowRegistry,
    private[spekka] val sharding: ClusterSharding
  )(implicit
    scheduler: Scheduler,
    ec: ExecutionContext,
    timeout: Timeout
  ) {

  /** Register a stateful flow for the specified entity kind.
    *
    * @param entityKind
    *   The entity kind associated to the flow
    * @param props
    *   The `StatefulFlowProps` of the flow
    * @return
    *   a `StatefulFlowBuilder` instance
    */
  def registerStatefulFlow[State, In, Out, Command](
      entityKind: String,
      props: StatefulFlowProps[In, Out, Command]
    ): Future[StatefulFlowBuilder[In, Out, Command]] = {
    for {
      builder <- registry.registerStatefulFlow(entityKind, props)
      entityTypeKey = EntityTypeKey[StatefulFlowHandler.Protocol[In, Out, Command, props.BP]](
        entityKind
      )
      ref = sharding.init(
        Entity(entityTypeKey)(ctx => props.behaviorFor(ctx.entityTypeKey.name, ctx.entityId))
      )
    } yield new ShardedStatefulFlowRegistry.StatefulFlowBuilderImpl(
      this,
      entityTypeKey.asInstanceOf[EntityTypeKey[
        StatefulFlowHandler.Protocol[In, Out, Command, Nothing]
      ]],
      ref.asInstanceOf[ActorRef[
        ShardingEnvelope[StatefulFlowHandler.Protocol[In, Out, Command, Nothing]]
      ]]
    )
  }

  /** Register a stateful flow for the specified entity kind, blocking the thread until the registry
    * completes the registration process.
    *
    * @param entityKind
    *   The entity kind associated to the flow
    * @param props
    *   The `StatefulFlowProps` of the flow
    * @return
    *   a `StatefulFlowBuilder` instance
    */
  def registerStatefulFlowSync[State, In, Out, Command](
      entityKind: String,
      props: StatefulFlowProps[In, Out, Command]
    ): StatefulFlowBuilder[In, Out, Command] =
    Await.result(
      registerStatefulFlow[State, In, Out, Command](entityKind, props),
      Duration.Inf
    )

  private[spekka] def makeFlow[FIn, In, Out, FOut, Command](
      builder: StatefulFlowBuilder[In, Out, Command],
      entityId: String,
      inputExtractor: FIn => In,
      outputBuilder: (FIn, Seq[Out]) => FOut
    ): Future[Flow[FIn, FOut, StatefulFlowControl[Command]]] = {
    builder match {
      case b: ShardedStatefulFlowRegistry.StatefulFlowBuilderImpl[In, Out, Command]
          if b.registry == this =>
        Future.successful {
          Flow[FIn]
            .map(i => inputExtractor(i) -> i)
            .viaMat(
              ActorFlow
                .askWithStatusAndContext[In, ShardingEnvelope[
                  StatefulFlowHandler.Protocol[In, Out, Command, Nothing]
                ], StatefulFlowHandler.ProcessFlowOutput[Out], FIn](
                  1
                )(b.shardingRef) { case (in, replyTo) =>
                  ShardingEnvelope(
                    entityId,
                    StatefulFlowHandler.ProcessFlowInput(in, replyTo)
                  )
                }
                .map { case (out, pass) => outputBuilder(pass, out.outs) }
                .mapMaterializedValue(_ =>
                  new ShardedStatefulFlowRegistry.StatefulFlowControlImpl(
                    entityId,
                    b.shardingRef.asInstanceOf[ActorRef[
                      ShardingEnvelope[StatefulFlowHandler.Protocol[Nothing, Any, Command, Nothing]]
                    ]]
                  )
                )
            )(Keep.right)
        }

      case _ =>
        Future.failed(
          new IllegalArgumentException(
            s"The supplied stateful flow handler reference for entity kind [${builder.entityKind}] was not registered with this registry"
          )
        )
    }

  }

  private[spekka] def makeControl[Command](
      builder: StatefulFlowBuilder[_, _, Command],
      entityId: String
    ): Future[Option[StatefulFlowControl[Command]]] = {
    builder match {
      case b: ShardedStatefulFlowRegistry.StatefulFlowBuilderImpl[_, _, Command]
          if b.registry == this =>
        Future.successful {
          Some(
            new ShardedStatefulFlowRegistry.StatefulFlowControlImpl(
              entityId,
              b.shardingRef.asInstanceOf[ActorRef[
                ShardingEnvelope[StatefulFlowHandler.Protocol[Nothing, Any, Command, Nothing]]
              ]]
            )
          )
        }
      case _ =>
        Future.failed(
          new IllegalArgumentException(
            s"The supplied stateful flow handler reference for entity kind [${builder.entityKind}] was not registered with this registry"
          )
        )
    }
  }

}

object ShardedStatefulFlowRegistry {
  final private[spekka] class StatefulFlowControlImpl[Command](
      entityId: String,
      shardingRef: ActorRef[
        ShardingEnvelope[StatefulFlowHandler.Protocol[Nothing, Any, Command, Nothing]]
      ]
    )(implicit
      scheduler: Scheduler,
      ec: ExecutionContext,
      timeout: Timeout
    ) extends StatefulFlowControl[Command] {

    import akka.actor.typed.scaladsl.AskPattern._

    def command(command: Command): Unit =
      shardingRef.tell(ShardingEnvelope(entityId, StatefulFlowHandler.ProcessCommand(command)))

    def commandWithResult[Result](f: ActorRef[StatusReply[Result]] => Command): Future[Result] =
      shardingRef.askWithStatus[Result](a =>
        ShardingEnvelope(entityId, StatefulFlowHandler.ProcessCommand(f(a)))
      )

    override def terminate(): Future[Done] =
      for {
        _ <- shardingRef.askWithStatus[Done](a =>
          ShardingEnvelope(entityId, StatefulFlowHandler.TerminateRequest(a))
        )
      } yield Done
  }

  final private[spekka] class StatefulFlowBuilderImpl[In, Out, Command](
      private[spekka] val registry: ShardedStatefulFlowRegistry,
      private[spekka] val entityType: EntityTypeKey[
        StatefulFlowHandler.Protocol[In, Out, Command, Nothing]
      ],
      private[spekka] val shardingRef: ActorRef[
        ShardingEnvelope[StatefulFlowHandler.Protocol[In, Out, Command, Nothing]]
      ]
    ) extends StatefulFlowBuilder[In, Out, Command] {

    val entityKind: String = entityType.name

    override def flow(
        entityId: String
      ): Flow[In, Seq[Out], Future[StatefulFlowControl[Command]]] = {
      Flow.futureFlow(
        registry.makeFlow[In, In, Out, Seq[Out], Command](
          this,
          entityId,
          identity,
          (_, outs) => outs
        )
      )
    }

    override def flowWithContext[Ctx](
        entityId: String
      ): FlowWithContext[In, Ctx, Seq[Out], Ctx, Future[StatefulFlowControl[Command]]] =
      FlowWithContext.fromTuples {
        Flow.futureFlow(
          registry.makeFlow[(In, Ctx), In, Out, (Seq[Out], Ctx), Command](
            this,
            entityId,
            in => in._1,
            (pass, outs) => outs -> pass._2
          )
        )
      }

    override def flowWithExtendedContext[Ctx](
        entityId: String
      ): FlowWithExtendedContext[In, Seq[Out], Ctx, Future[StatefulFlowControl[Command]]] = {
      FlowWithExtendedContext.fromGraphUnsafe(
        Flow.futureFlow(
          registry.makeFlow[
            (In, ExtendedContext[Ctx]),
            In,
            Out,
            (Seq[Out], ExtendedContext[Ctx]),
            Command
          ](
            this,
            entityId,
            in => in._1,
            (pass, outs) => outs -> pass._2
          )
        )
      )
    }

    def control(entityId: String): Future[Option[StatefulFlowControl[Command]]] =
      registry.makeControl(this, entityId)

    override def lazyControl(implicit ec: ExecutionContext): StatefulFlowLazyControl[Command] =
      new StatefulFlowRegistry.StatefulFlowLazyControlImpl(this)

    override def lazyEntityControl(
        entityId: String
      )(implicit ec: ExecutionContext
      ): StatefulFlowLazyEntityControl[Command] =
      new StatefulFlowRegistry.StatefulFlowLazyControlImpl(this).narrow(entityId)
  }

  /** Creates a [[ShardedStatefulFlowRegistry]].
    *
    * @param registry
    *   The base `StatefulFlowRegistry`
    * @param sharding
    *   The Akka Cluster Sharding extension
    * @param timeout
    *   Timeout for interaction with the registry
    * @return
    *   [[ShardedStatefulFlowRegistry]]
    */
  def apply(
      registry: StatefulFlowRegistry,
      sharding: ClusterSharding,
      timeout: Timeout
    )(implicit systemProvider: StatefulFlowRegistry.ActorSystemProvider
    ): ShardedStatefulFlowRegistry = {
    val system = systemProvider.system
    new ShardedStatefulFlowRegistry(registry, sharding)(
      system.scheduler,
      system.executionContext,
      timeout
    )
  }
}
