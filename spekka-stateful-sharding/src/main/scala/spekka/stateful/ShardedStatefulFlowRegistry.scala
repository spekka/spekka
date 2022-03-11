package spekka.stateful

import akka.Done
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorSystem
import akka.actor.typed.Scheduler
import akka.cluster.sharding.typed.ShardingEnvelope
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.sharding.typed.scaladsl.Entity
import akka.cluster.sharding.typed.scaladsl.EntityTypeKey
import akka.pattern.StatusReply
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.FlowWithContext
import akka.stream.typed.scaladsl.ActorFlow
import akka.util.Timeout

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration

class ShardedStatefulFlowRegistry(
    private[spekka] val registry: StatefulFlowRegistry,
    private[spekka] val sharding: ClusterSharding
  )(implicit scheduler: Scheduler,
    ec: ExecutionContext,
    timeout: Timeout) {

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
          ActorFlow
            .askWithStatus[FIn, ShardingEnvelope[
              StatefulFlowHandler.Protocol[In, Out, Command, Nothing]
            ], (Seq[Out], FIn)](
              1
            )(b.shardingRef) { case (in, replyTo) =>
              ShardingEnvelope(
                entityId,
                StatefulFlowHandler.ProcessFlowInput(inputExtractor(in), in, replyTo)
              )
            }
            .map { case (outs, pass) => outputBuilder(pass, outs) }
            .mapMaterializedValue(_ =>
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
    )(implicit scheduler: Scheduler,
      ec: ExecutionContext,
      timeout: Timeout)
      extends StatefulFlowControl[Command] {

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
      ])
      extends StatefulFlowBuilder[In, Out, Command] {

    val entityKind: String = entityType.name

    def flow(
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

    def flowWithContext[Ctx](
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

    def control(entityId: String): Future[Option[StatefulFlowControl[Command]]] =
      registry.makeControl(this, entityId)
  }

  def apply(
      registry: StatefulFlowRegistry,
      sharding: ClusterSharding,
      timeout: Timeout
    )(implicit system: ActorSystem[_]
    ): ShardedStatefulFlowRegistry =
    new ShardedStatefulFlowRegistry(registry, sharding)(
      system.scheduler,
      system.executionContext,
      timeout
    )
}
