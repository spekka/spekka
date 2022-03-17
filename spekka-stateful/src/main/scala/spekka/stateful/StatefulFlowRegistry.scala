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
import akka.actor.typed.ActorSystem
import akka.actor.typed.Behavior
import akka.actor.typed.Scheduler
import akka.actor.typed.scaladsl.Behaviors
import akka.pattern.StatusReply
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.FlowWithContext
import akka.stream.typed.scaladsl.ActorFlow
import akka.util.Timeout

import scala.concurrent.Await
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.concurrent.duration.Duration

/**
  * A [[StatefulFlowRegistry]] is responsible of handling the materialization of stateful flows.
  *
  * Each stateful flow manages a specific kind on entities. Once a flow has been registered for a particular 
  * ''entityKind'', the resulting builder object can be used to instantiate flows for specific ''entities''.
  *
  * {{{
  *   val registry = StatefulFlowRegistry(30.seconds)
  *
  *   val flowProps: StatefulFlowProps[In, Out, Command] = ???
  *
  *   val builder = registry.registerStatefulFlowSync("entity-kind", flowProps)
  *   builder.flow("entity-id")
  * }}}
  *
  * It is recommended to use a single registry for each application, however in case this were not possible
  * (for instance because the registry needs to be created in a library or self-contained module) it is possible 
  * to specify a name at construction time to differentiate multiple instances. If a registry with the same name
  * already exists an exception will be thrown.
  *
  * Note that when using multiple registries it is the responsibility of the programmer to make sure that
  * stateful flows registered on different instances do not uses the same storage with the same entity kind.
  */
class StatefulFlowRegistry private[spekka] (
    private val registryRef: ActorRef[StatefulFlowRegistry.ExposedProtocol]
  )(implicit scheduler: Scheduler,
    ec: ExecutionContext,
    timeout: Timeout) {
  import akka.actor.typed.scaladsl.AskPattern._

  /**
    * Register a stateful flow for the specified entity kind.
    *
    * @param entityKind The entity kind associated to the flow
    * @param props The [[StatefulFlowProps]] of the flow
    * @return a [[StatefulFlowBuilder]] instance
    */
  def registerStatefulFlow[State, In, Out, Command](
      entityKind: String,
      props: StatefulFlowProps[In, Out, Command]
    ): Future[StatefulFlowBuilder[In, Out, Command]] =
    registryRef
      .askWithStatus[Done](
        StatefulFlowRegistry.Protocol.RegisterStatefulFlowHandler(entityKind, props, _)
      )
      .map(_ => new StatefulFlowRegistry.StatefulFlowBuilderImpl(this, entityKind))

  /**
    * Register a stateful flow for the specified entity kind, blocking the thread until
    * the registry completes the registration process.
    *
    * @param entityKind The entity kind associated to the flow
    * @param props The [[StatefulFlowProps]] of the flow
    * @return a [[StatefulFlowBuilder]] instance
    */
  def registerStatefulFlowSync[State, In, Out, Command](
      entityKind: String,
      props: StatefulFlowProps[In, Out, Command]
    ): StatefulFlowBuilder[In, Out, Command] =
    Await.result(
      registerStatefulFlow[State, In, Out, Command](entityKind, props),
      Duration.Inf
    )

  private[spekka] def getHandler[In, Out, Command](
      builder: StatefulFlowBuilder[In, Out, Command],
      entityId: String
    ): Future[Option[ActorRef[StatefulFlowHandler.Protocol[In, Out, Command, Nothing]]]] =
    builder match {
      case b: StatefulFlowRegistry.StatefulFlowBuilderImpl[In, Out, Command]
          if b.registry.registryRef == registryRef =>
        registryRef
          .askWithStatus(
            StatefulFlowRegistry.Protocol
              .GetHandlerForEntity(builder.entityKind, entityId, _)
          )
          .mapTo[Option[ActorRef[StatefulFlowHandler.Protocol[In, Out, Command, Nothing]]]]

      case _ =>
        Future.failed(
          new IllegalArgumentException(
            s"The supplied stateful flow handler reference for entity kind [${builder.entityKind}] was not registered with this registry"
          )
        )
    }

  private[spekka] def getOrElseSpawnHandler[In, Out, Command, BackendProtocol](
      builder: StatefulFlowBuilder[In, Out, Command],
      entityId: String
    ): Future[ActorRef[StatefulFlowHandler.Protocol[In, Out, Command, BackendProtocol]]] =
    builder match {
      case b: StatefulFlowRegistry.StatefulFlowBuilderImpl[In, Out, Command]
          if b.registry.registryRef == registryRef =>
        registryRef
          .askWithStatus(
            StatefulFlowRegistry.Protocol
              .GetOrSpawnHandlerForEntity(builder.entityKind, entityId, _)
          )
          .mapTo[ActorRef[StatefulFlowHandler.Protocol[In, Out, Command, BackendProtocol]]]

      case _ =>
        Future.failed(
          new IllegalArgumentException(
            s"The supplied stateful flow handler reference for entity kind [${builder.entityKind}] was not registered with this registry"
          )
        )
    }

  private[spekka] def waitForTermination(entityKind: String, entityId: String): Future[Done] =
    registryRef.ask[Done](
      StatefulFlowRegistry.Protocol.WaitForHandlerTermination(entityKind, entityId, _)
    )

  private[spekka] def makeFlow[FIn, In, Out, FOut, Command](
      builder: StatefulFlowBuilder[In, Out, Command],
      entityId: String,
      inputExtractor: FIn => In,
      outputBuilder: (FIn, Seq[Out]) => FOut
    ): Future[Flow[FIn, FOut, StatefulFlowControl[Command]]] =
    getOrElseSpawnHandler(builder, entityId).map { flowHandlerRef =>
      ActorFlow
        .askWithStatus[FIn, StatefulFlowHandler.ProcessFlowInput[In, Out, FIn], (Seq[Out], FIn)](
          1
        )(flowHandlerRef) { case (in, replyTo) =>
          StatefulFlowHandler.ProcessFlowInput(inputExtractor(in), in, replyTo)
        }
        .map { case (outs, pass) => outputBuilder(pass, outs) }
        .mapMaterializedValue(_ =>
          new StatefulFlowRegistry.StatefulFlowControlImpl(
            this,
            builder.entityKind,
            entityId,
            flowHandlerRef
          )
        )
    }

  private[spekka] def makeControl[Command](
      builder: StatefulFlowBuilder[_, _, Command],
      entityId: String
    ): Future[Option[StatefulFlowControl[Command]]] = {
    getHandler(builder, entityId).map {
      case Some(flowHandlerRef) =>
        Some(
          new StatefulFlowRegistry.StatefulFlowControlImpl(
            this,
            builder.entityKind,
            entityId,
            flowHandlerRef
          )
        )
      case None => None
    }
  }
}

object StatefulFlowRegistry {
  private[spekka] sealed trait Protocol
  private[spekka] sealed trait ExposedProtocol extends Protocol

  final private[spekka] class StatefulFlowControlImpl[Command](
      registry: StatefulFlowRegistry,
      entityKind: String,
      entityId: String,
      handlerRef: ActorRef[StatefulFlowHandler.Protocol[Nothing, Any, Command, Any]]
    )(implicit scheduler: Scheduler,
      ec: ExecutionContext,
      timeout: Timeout)
      extends StatefulFlowControl[Command] {
    import akka.actor.typed.scaladsl.AskPattern._

    def command(command: Command): Unit =
      handlerRef.tell(StatefulFlowHandler.ProcessCommand(command))

    def commandWithResult[Result](f: ActorRef[StatusReply[Result]] => Command): Future[Result] =
      handlerRef.askWithStatus[Result](a => StatefulFlowHandler.ProcessCommand(f(a)))

    def terminate(): Future[Done] =
      for {
        _ <- handlerRef.askWithStatus[Done](StatefulFlowHandler.TerminateRequest)
        _ <- registry.waitForTermination(entityKind, entityId)
      } yield Done
  }

  private[spekka] final class StatefulFlowBuilderImpl[In, Out, Command](
      private[spekka] val registry: StatefulFlowRegistry,
      val entityKind: String)
      extends StatefulFlowBuilder[In, Out, Command] {
    def flow(
        entityId: String
      ): Flow[In, Seq[Out], Future[StatefulFlowControl[Command]]] =
      Flow.lazyFutureFlow { () =>
        registry.makeFlow[In, In, Out, Seq[Out], Command](
          this,
          entityId,
          identity,
          (_, outs) => outs
        )
      }

    def flowWithContext[Ctx](
        entityId: String
      ): FlowWithContext[In, Ctx, Seq[Out], Ctx, Future[StatefulFlowControl[Command]]] =
      FlowWithContext.fromTuples {
        Flow.lazyFutureFlow { () =>
          registry.makeFlow[(In, Ctx), In, Out, (Seq[Out], Ctx), Command](
            this,
            entityId,
            in => in._1,
            (pass, outs) => outs -> pass._2
          )
        }
      }

    def control(entityId: String): Future[Option[StatefulFlowControl[Command]]] =
      registry.makeControl(this, entityId)
  }

  private[spekka] object Protocol {
    case class RegisterStatefulFlowHandler[In, Out, Command, Result](
        entityKind: String,
        flowSpec: StatefulFlowProps[In, Out, Command],
        replyTo: ActorRef[StatusReply[Done]])
        extends ExposedProtocol

    case class GetHandlerForEntity(
        entityKind: String,
        entityId: String,
        replyTo: ActorRef[StatusReply[Option[ActorRef[StatefulFlowHandler.Protocol[_, _, _, _]]]]])
        extends ExposedProtocol

    case class GetOrSpawnHandlerForEntity(
        entityKind: String,
        entityId: String,
        replyTo: ActorRef[StatusReply[ActorRef[StatefulFlowHandler.Protocol[_, _, _, _]]]])
        extends ExposedProtocol

    case class WaitForHandlerTermination(
        entityKind: String,
        entityId: String,
        replyTo: ActorRef[Done])
        extends ExposedProtocol

    case class StatefulFlowHandlerTerminated(entityKind: String, entityId: String) extends Protocol
  }

  private def registryBehavior(
      propsMap: Map[String, StatefulFlowProps[_, _, _]],
      instanceMap: Map[(String, String), ActorRef[StatefulFlowHandler.Protocol[_, _, _, _]]],
      instanceTerminationWatchers: Map[(String, String), List[ActorRef[Done]]]
    ): Behavior[Protocol] =
    Behaviors.setup[Protocol] { actorContext =>
      Behaviors.receiveMessage[Protocol] {
        case Protocol.RegisterStatefulFlowHandler(entityKind, flowSpec, replyTo) =>
          if (propsMap.contains(entityKind)) {
            replyTo.tell(
              StatusReply.error(
                s"Stateful flow for id entity kind [${entityKind}] already registered!"
              )
            )
            Behaviors.same
          } else {
            actorContext.log
              .info(
                s"Registered stateful flow for entity kind [${entityKind}] with backend [${flowSpec.backend.id}]"
              )
            replyTo.tell(StatusReply.ack())
            registryBehavior(
              propsMap + (entityKind -> flowSpec),
              instanceMap,
              instanceTerminationWatchers
            )
          }

        case Protocol.GetHandlerForEntity(entityKind, entityId, replyTo) =>
          replyTo ! StatusReply.success(instanceMap.get(entityKind -> entityId))
          Behaviors.same

        case Protocol.GetOrSpawnHandlerForEntity(entityKind, entityId, replyTo) =>
          instanceMap.get(entityKind -> entityId) match {
            case Some(ref) =>
              replyTo.tell(StatusReply.success(ref))
              Behaviors.same

            case None =>
              propsMap.get(entityKind) match {
                case Some(spec) =>
                  val ref = actorContext
                    .spawn(
                      spec.behaviorFor(entityKind, entityId),
                      s"handler-${entityKind}-${entityId}"
                    )
                    .asInstanceOf[ActorRef[StatefulFlowHandler.Protocol[_, _, _, _]]]
                  actorContext.watchWith(
                    ref,
                    Protocol.StatefulFlowHandlerTerminated(entityKind, entityId)
                  )
                  replyTo.tell(StatusReply.success(ref))
                  actorContext.log
                    .info(
                      s"Spawned stateful flow instance for entity kind [${entityKind}] with entity id [${entityId}]"
                    )
                  registryBehavior(
                    propsMap,
                    instanceMap + ((entityKind, entityId) -> ref),
                    instanceTerminationWatchers
                  )

                case None =>
                  replyTo.tell(
                    StatusReply.error(
                      new NoSuchElementException(
                        s"No handler registered for entity kind [${entityKind}]"
                      )
                    )
                  )
                  Behaviors.same
              }
          }

        case Protocol.WaitForHandlerTermination(entityKind, entityId, replyTo) =>
          if (instanceMap.contains(entityKind -> entityId)) {
            val currentWatchers = instanceTerminationWatchers.getOrElse(entityKind -> entityId, Nil)
            val updatedWatchers = replyTo :: currentWatchers
            val updatedWatchersMap =
              instanceTerminationWatchers.updated(entityKind -> entityId, updatedWatchers)
            registryBehavior(propsMap, instanceMap, updatedWatchersMap)
          } else {
            replyTo.tell(Done)
            Behaviors.same
          }

        case Protocol.StatefulFlowHandlerTerminated(entityKind, entityId) =>
          actorContext.log
            .info(
              s"Stateful flow instance for entity kind [${entityKind}] with entity id [${entityId}] terminated"
            )
          instanceTerminationWatchers.getOrElse(entityKind -> entityId, Nil).foreach(_.tell(Done))
          registryBehavior(
            propsMap,
            instanceMap - (entityKind -> entityId),
            instanceTerminationWatchers - (entityKind -> entityId)
          )
      }
    }

  /**
    * Creates a [[StatefulFlowRegistry]].
    *
    * @param queryTimeout Timeout for interactions with the registry
    * @param name Name of the registry
    * @return [[StatefulFlowRegistry]] instance
    */
  def apply(
      queryTimeout: Timeout,
      name: String = "default"
    )(implicit system: ActorSystem[_]
    ): StatefulFlowRegistry = {
    val ref = system
      .systemActorOf(
        registryBehavior(Map.empty, Map.empty, Map.empty),
        s"stateful-flow-registry-${name}"
      )
    new StatefulFlowRegistry(ref)(system.scheduler, system.executionContext, queryTimeout)
  }

}
