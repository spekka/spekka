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

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.scaladsl.Behaviors
import akka.pattern.StatusReply
import akka.persistence.typed.EventAdapter
import akka.persistence.typed.EventSeq
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.SnapshotAdapter
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.persistence.typed.scaladsl.RetentionCriteria
import akka.persistence.typed.state.scaladsl.DurableStateBehavior
import akka.serialization.SerializerWithStringManifest
import spekka.codec.Codec

import scala.concurrent.ExecutionContext
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object AkkaPersistenceStatefulFlowBackend {

  class SerializedData(val bytes: Array[Byte])
  class SerializedDataSerializer extends SerializerWithStringManifest {
    override def identifier: Int = Int.MaxValue
    override def manifest(o: AnyRef): String = ""
    override def toBinary(o: AnyRef): Array[Byte] =
      o match {
        case s: SerializedData =>
          s.bytes
        case _ =>
          throw new IllegalArgumentException(
            s"${this.getClass().getName()} binded to wrong data-type. Expected ${classOf[SerializedData].getName()}"
          )
      }

    override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef =
      new SerializedData(bytes)
  }

  class StateWrapper[State] private[StateWrapper] (
      var waitForRecoverCompletion: Boolean,
      var waitingForProcessingCompletion: Boolean,
      val innerState: State) {
    def withInnerState(s: State): StateWrapper[State] =
      new StateWrapper(waitForRecoverCompletion, waitingForProcessingCompletion, s)
  }

  object StateWrapper {
    def recoveredState[State](s: State): StateWrapper[State] = new StateWrapper(true, false, s)
  }

  class SerializedDataSnapshotAdapter[T](codec: Codec[T]) extends SnapshotAdapter[T] {
    override def toJournal(state: T): Any = new SerializedData(codec.encoder.encode(state))
    override def fromJournal(from: Any): T =
      from match {
        case s: SerializedData => codec.decoder.decode(s.bytes).fold(throw _, state => state)
        case _ =>
          throw new IllegalArgumentException(
            s"${this.getClass().getName()} binded to wrong data-type. Expected ${classOf[SerializedData].getName()}"
          )
      }
  }

  private def defaultSnapshotAdapter[State] =
    new SnapshotAdapter[StateWrapper[State]] {
      override def toJournal(state: StateWrapper[State]): Any = state.innerState

      override def fromJournal(from: Any): StateWrapper[State] =
        StateWrapper.recoveredState(from.asInstanceOf[State])
    }

  private def makeWrappedSnapshotAdapted[State](
      adapter: SnapshotAdapter[State]
    ): SnapshotAdapter[StateWrapper[State]] = {
    new SnapshotAdapter[StateWrapper[State]] {
      override def toJournal(state: StateWrapper[State]): Any = adapter.toJournal(state.innerState)

      override def fromJournal(from: Any): StateWrapper[State] =
        StateWrapper.recoveredState(adapter.fromJournal(from))
    }
  }

  object EventBased {
    import akka.persistence.typed.scaladsl.Effect
    import akka.persistence.typed.scaladsl.EffectBuilder
    import akka.persistence.typed.RecoveryFailed
    import akka.persistence.typed.SnapshotCompleted
    import akka.persistence.typed.SnapshotFailed
    import akka.persistence.typed.RecoveryCompleted

    sealed private[spekka] trait AkkaPersistenceBackendProtocol
        extends StatefulFlowHandler.BackendProtocol[AkkaPersistenceBackendProtocol]

    private[spekka] case class BeforeSideEffectCompleted[Ev](
        result: StatefulFlowLogic.EventBased.ProcessingResult[Ev],
        successAction: () => Unit,
        failureAction: (Throwable) => Unit)
        extends AkkaPersistenceBackendProtocol

    private[spekka] case class AfterSideEffectCompleted(
        successAction: () => Unit)
        extends AkkaPersistenceBackendProtocol

    private[spekka] case class SideEffectFailure(ex: Throwable, failureAction: (Throwable) => Unit)
        extends AkkaPersistenceBackendProtocol

    trait PersistencePlugin {
      def journalPluginId: String
      def snapshotPluginId: String
    }

    object PersistencePlugin {
      case object CassandraStoragePlugin extends PersistencePlugin {
        override val journalPluginId: String = "akka.persistence.cassandra.journal"
        override val snapshotPluginId: String = "akka.persistence.cassandra.snapshot"
      }

      case object JdbcStoragePlugin extends PersistencePlugin {
        override val journalPluginId: String = "jdbc-journal"
        override val snapshotPluginId: String = "jdbc-snapshot-store"
      }

      case class CustomStoragePlugin(journalPluginId: String, snapshotPluginId: String)
          extends PersistencePlugin
    }

    class SerializedDataEventAdapter[T](codec: Codec[T]) extends EventAdapter[T, SerializedData] {
      override def manifest(event: T): String = ""
      override def toJournal(e: T): SerializedData = new SerializedData(codec.encoder.encode(e))

      override def fromJournal(p: SerializedData, manifest: String): EventSeq[T] =
        codec.decoder.decode(p.bytes).fold(throw _, e => EventSeq.single(e))
    }

    private[spekka] def behaviorFactory[State, Ev, In, Command](
        entityKind: String,
        entityId: String,
        logic: StatefulFlowLogic.EventBased[State, Ev, In, Command],
        retentionCriteria: RetentionCriteria,
        storagePlugin: PersistencePlugin,
        partitions: Int,
        sideEffectsParallelism: Int,
        eventAdapter: Option[EventAdapter[Ev, _]],
        snapshotAdapter: Option[SnapshotAdapter[State]]
      ): Behavior[StatefulFlowHandler.Protocol[In, Ev, Command, AkkaPersistenceBackendProtocol]] = {
      val partitionId = Math.abs(entityId.hashCode % partitions)
      val partitionTag = Set(s"$entityKind-$partitionId")

      def handleResult(
          self: ActorRef[
            StatefulFlowHandler.Protocol[In, Ev, Command, AkkaPersistenceBackendProtocol]
          ],
          state: StateWrapper[State],
          result: StatefulFlowLogic.EventBased.ProcessingResult[Ev],
          successAction: () => Unit,
          failureAction: (Throwable) => Unit
        )(implicit ec: ExecutionContext
        ): EffectBuilder[Ev, StateWrapper[State]] = {
        if (!state.waitForRecoverCompletion && result.hasSideEffects) {
          state.waitingForProcessingCompletion = true

          if (result.beforeUpdateSideEffects.nonEmpty) {
            SideEffectRunner
              .run(result.beforeUpdateSideEffects, sideEffectsParallelism)
              .onComplete {
                case Success(_) =>
                  self ! BeforeSideEffectCompleted(
                    result,
                    successAction,
                    failureAction
                  )
                case Failure(ex) =>
                  self ! SideEffectFailure(ex, failureAction)
              }
            Effect.none
          } else {
            Effect
              .persist(result.events)
              .thenRun((_: StateWrapper[State]) =>
                SideEffectRunner
                  .run(result.afterUpdateSideEffects, sideEffectsParallelism)
                  .onComplete {
                    case Success(_) =>
                      self ! AfterSideEffectCompleted(successAction)
                    case Failure(ex) =>
                      self ! SideEffectFailure(ex, failureAction)
                  }
              )
          }
        } else {
          Effect
            .persist(result.events)
            .thenRun((_: StateWrapper[State]) => successAction())
        }
      }

      Behaviors.setup { actorContext =>
        implicit val ec: scala.concurrent.ExecutionContext = actorContext.executionContext

        val baseBehavior =
          EventSourcedBehavior[
            StatefulFlowHandler.Protocol[In, Ev, Command, AkkaPersistenceBackendProtocol],
            Ev,
            StateWrapper[State]
          ](
            PersistenceId(entityKind, entityId),
            StateWrapper.recoveredState(logic.initialState),
            (state, req) =>
              req match {
                case BeforeSideEffectCompleted(
                      result: StatefulFlowLogic.EventBased.ProcessingResult[Ev @unchecked],
                      successAction,
                      failureAction
                    ) =>
                  if (result.afterUpdateSideEffects.nonEmpty) {
                    Effect
                      .persist(result.events)
                      .thenRun { _ =>
                        SideEffectRunner
                          .run(result.afterUpdateSideEffects, sideEffectsParallelism)
                          .onComplete {
                            case Success(_) =>
                              actorContext.self ! AfterSideEffectCompleted(successAction)
                            case Failure(ex) =>
                              actorContext.self ! SideEffectFailure(ex, failureAction)
                          }
                      }
                  } else {
                    Effect
                      .persist(result.events)
                      .thenRun((s: StateWrapper[State]) => s.waitingForProcessingCompletion = false)
                      .thenRun(_ => successAction())
                      .thenUnstashAll()
                  }

                case AfterSideEffectCompleted(successAction) =>
                  state.waitingForProcessingCompletion = false
                  successAction()
                  Effect.unstashAll()

                case SideEffectFailure(ex, failureAction) =>
                  state.waitingForProcessingCompletion = false
                  actorContext.log.error("Failure handling processing side effects", ex)
                  failureAction(ex)
                  Effect.none

                case flowInput: StatefulFlowHandler.ProcessFlowInput[In, Ev, _] =>
                  if (!state.waitingForProcessingCompletion) {
                    Try(logic.processInput(state.innerState, flowInput.in)) match {
                      case Success(result) =>
                        val successAction = () =>
                          flowInput.replyTo ! StatusReply.success(
                            result.events -> flowInput.passthrough
                          )
                        val failureAction =
                          (ex: Throwable) => flowInput.replyTo ! StatusReply.error(ex)

                        handleResult(
                          actorContext.self,
                          state,
                          result,
                          successAction,
                          failureAction
                        )

                      case Failure(ex) =>
                        actorContext.log.error("Failure handling input", ex)
                        Effect.reply(flowInput.replyTo)(StatusReply.error(ex))
                    }
                  } else Effect.stash()

                case commandRequest: StatefulFlowHandler.ProcessCommand[Command] =>
                  if (!state.waitingForProcessingCompletion) {
                    Try(logic.processCommand(state.innerState, commandRequest.command)) match {
                      case Success(result) =>
                        val successAction = () => ()
                        val failureAction = (_: Throwable) => ()

                        handleResult(
                          actorContext.self,
                          state,
                          result,
                          successAction,
                          failureAction
                        )

                      case Failure(ex) =>
                        actorContext.log.error("Failure handling command", ex)
                        Effect.none
                    }
                  } else Effect.stash()

                case StatefulFlowHandler.TerminateRequest(replyTo) =>
                  if (!state.waitingForProcessingCompletion) {
                    replyTo.tell(StatusReply.ack())
                    Effect.stop()
                  } else Effect.stash()

                case msg =>
                  if (!state.waitingForProcessingCompletion) {
                    actorContext.log.error(
                      s"Inconsistency in AkkaPersistenceStatefulFlowBackend.EventBased. Received unexpected message: ${msg}"
                    )
                    Effect.stop()
                  } else Effect.stash()
              },
            (state, event) => state.withInnerState(logic.updateState(state.innerState, event))
          ).withRetention(retentionCriteria)
            .withTagger(_ => partitionTag)
            .withJournalPluginId(storagePlugin.journalPluginId)
            .withSnapshotPluginId(storagePlugin.snapshotPluginId)
            .receiveSignal {
              case (state, _: RecoveryCompleted) =>
                state.waitForRecoverCompletion = false
                actorContext.log.info(s"Recovery completed with state: ${state.innerState}")
              case (_, RecoveryFailed(ex)) => actorContext.log.error("Recovery failed", ex)
              case (_, c: SnapshotCompleted) =>
                if (actorContext.log.isDebugEnabled()) {
                  actorContext.log.debug(s"Snapshot completed with metadata: ${c.metadata}")
                }
              case (_, SnapshotFailed(metadata, ex)) =>
                actorContext.log.error(s"Snapshot failed for metadata ${metadata}", ex)
            }

        val behaviorWithEventAdapter =
          eventAdapter.map(baseBehavior.eventAdapter).getOrElse(baseBehavior)

        val behaviorWithEventAndSnapshotAdapter = snapshotAdapter
          .map(a => behaviorWithEventAdapter.snapshotAdapter(makeWrappedSnapshotAdapted(a)))
          .getOrElse(behaviorWithEventAdapter.snapshotAdapter(defaultSnapshotAdapter))

        behaviorWithEventAndSnapshotAdapter
      }
    }

    def apply[State, Ev](
        storagePlugin: EventBased.PersistencePlugin,
        retentionCriteria: RetentionCriteria = RetentionCriteria.disabled,
        eventsPartitions: Int = 1,
        sideEffectsParallelism: Int = 1
      ): EventBased[State, Ev] =
      new EventBased(
        storagePlugin,
        retentionCriteria,
        eventsPartitions,
        sideEffectsParallelism,
        None,
        None
      )
  }

  class EventBased[State, Ev] private[spekka] (
      storagePlugin: EventBased.PersistencePlugin,
      retentionCriteria: RetentionCriteria,
      eventsPartitions: Int,
      sideEffectsParallelism: Int,
      eventAdapter: Option[EventAdapter[Ev, _]],
      snapshotAdapter: Option[SnapshotAdapter[State]])
      extends StatefulFlowBackend.EventBased[State, Ev, EventBased.AkkaPersistenceBackendProtocol] {
    override val id: String = "akka-persistence-event-based"

    override def behaviorFor[In, Command](
        logic: StatefulFlowLogic.EventBased[State, Ev, In, Command],
        entityKind: String,
        entityId: String
      ): Behavior[StatefulFlowHandler.Protocol[In, Ev, Command, EventBased.AkkaPersistenceBackendProtocol]] =
      EventBased.behaviorFactory(
        entityKind,
        entityId,
        logic,
        retentionCriteria,
        storagePlugin,
        eventsPartitions,
        sideEffectsParallelism,
        eventAdapter,
        snapshotAdapter
      )

    def withEventsPartitions(n: Int): EventBased[State, Ev] =
      new EventBased(
        storagePlugin,
        retentionCriteria,
        n,
        sideEffectsParallelism,
        eventAdapter,
        snapshotAdapter
      )

    def withSideEffectsParallelism(n: Int): EventBased[State, Ev] =
      new EventBased(
        storagePlugin,
        retentionCriteria,
        eventsPartitions,
        n,
        eventAdapter,
        snapshotAdapter
      )

    def withEventAdapter(
        eventAdapter: EventAdapter[Ev, _]
      ): EventBased[State, Ev] =
      new EventBased[State, Ev](
        storagePlugin,
        retentionCriteria,
        eventsPartitions,
        sideEffectsParallelism,
        Some(eventAdapter),
        snapshotAdapter
      )

    def withSnapshotAdapter(
        snapshotAdapter: SnapshotAdapter[State]
      ): EventBased[State, Ev] =
      new EventBased[State, Ev](
        storagePlugin,
        retentionCriteria,
        eventsPartitions,
        sideEffectsParallelism,
        eventAdapter,
        Some(snapshotAdapter)
      )

    def withEventCodec(
        implicit eventCodec: Codec[Ev]
      ): EventBased[State, Ev] =
      new EventBased[State, Ev](
        storagePlugin,
        retentionCriteria,
        eventsPartitions,
        sideEffectsParallelism,
        Some(new EventBased.SerializedDataEventAdapter(eventCodec)),
        snapshotAdapter
      )

    def withSnapshotCodec(
        implicit snapshotCodec: Codec[State]
      ): EventBased[State, Ev] =
      new EventBased[State, Ev](
        storagePlugin,
        retentionCriteria,
        eventsPartitions,
        sideEffectsParallelism,
        eventAdapter,
        Some(new SerializedDataSnapshotAdapter(snapshotCodec))
      )
  }

  object DurableState {
    import akka.persistence.typed.state.scaladsl.Effect
    import akka.persistence.typed.state.scaladsl.EffectBuilder
    import akka.persistence.typed.state.RecoveryFailed
    import akka.persistence.typed.state.RecoveryCompleted

    sealed private[spekka] trait AkkaPersistenceBackendProtocol
        extends StatefulFlowHandler.BackendProtocol[AkkaPersistenceBackendProtocol]

    private[spekka] case class BeforeSideEffectCompleted[State, Out](
        result: StatefulFlowLogic.DurableState.ProcessingResult[State, Out],
        successAction: () => Unit,
        failureAction: (Throwable) => Unit)
        extends AkkaPersistenceBackendProtocol

    private[spekka] case class AfterSideEffectCompleted(
        successAction: () => Unit)
        extends AkkaPersistenceBackendProtocol

    private[spekka] case class SideEffectFailure(ex: Throwable, failureAction: (Throwable) => Unit)
        extends AkkaPersistenceBackendProtocol

    trait PersistencePlugin {
      def statePluginId: String
    }

    object PersistencePlugin {
      case object JdbcStoragePlugin extends PersistencePlugin {
        override val statePluginId: String = "jdbc-durable-state-store"
      }

      case class CustomStoragePlugin(statePluginId: String) extends PersistencePlugin
    }

    private[spekka] def behaviorFactory[State, In, Out, Command](
        entityKind: String,
        entityId: String,
        logic: StatefulFlowLogic.DurableState[State, In, Out, Command],
        storagePlugin: PersistencePlugin,
        sideEffectsParallelism: Int,
        snapshotAdapter: Option[SnapshotAdapter[State]]
      ): Behavior[StatefulFlowHandler.Protocol[In, Out, Command, AkkaPersistenceBackendProtocol]] = {

      def handleResult(
          self: ActorRef[
            StatefulFlowHandler.Protocol[In, Out, Command, AkkaPersistenceBackendProtocol]
          ],
          state: StateWrapper[State],
          result: StatefulFlowLogic.DurableState.ProcessingResult[State, _],
          successAction: () => Unit,
          failureAction: (Throwable) => Unit
        )(implicit ec: ExecutionContext
        ): EffectBuilder[StateWrapper[State]] = {
        if (!state.waitForRecoverCompletion && result.hasSideEffects) {
          state.waitingForProcessingCompletion = true

          if (result.beforeUpdateSideEffects.nonEmpty) {
            SideEffectRunner
              .run(result.beforeUpdateSideEffects, sideEffectsParallelism)
              .onComplete {
                case Success(_) =>
                  self ! BeforeSideEffectCompleted(
                    result,
                    successAction,
                    failureAction
                  )
                case Failure(ex) =>
                  self ! SideEffectFailure(ex, failureAction)
              }
            Effect.none
          } else {
            Effect
              .persist(state.withInnerState(result.state))
              .thenRun((_: StateWrapper[State]) =>
                SideEffectRunner
                  .run(result.afterUpdateSideEffects, sideEffectsParallelism)
                  .onComplete {
                    case Success(_) =>
                      self ! AfterSideEffectCompleted(successAction)
                    case Failure(ex) =>
                      self ! SideEffectFailure(ex, failureAction)
                  }
              )
          }
        } else {
          Effect
            .persist(state.withInnerState(result.state))
            .thenRun((_: StateWrapper[State]) => successAction())
        }
      }

      Behaviors.setup { actorContext =>
        implicit val ec: scala.concurrent.ExecutionContext = actorContext.executionContext

        val baseBehavior = DurableStateBehavior[
          StatefulFlowHandler.Protocol[In, Out, Command, AkkaPersistenceBackendProtocol],
          StateWrapper[State]
        ](
          PersistenceId(entityKind, entityId),
          StateWrapper.recoveredState(logic.initialState),
          (state, req) =>
            req match {
              case BeforeSideEffectCompleted(
                    result: StatefulFlowLogic.DurableState.ProcessingResult[
                      State @unchecked,
                      Out @unchecked
                    ],
                    successAction,
                    failureAction
                  ) =>
                if (result.afterUpdateSideEffects.nonEmpty) {
                  Effect
                    .persist(state.withInnerState(result.state))
                    .thenRun { _ =>
                      SideEffectRunner
                        .run(result.afterUpdateSideEffects, sideEffectsParallelism)
                        .onComplete {
                          case Success(_) =>
                            actorContext.self ! AfterSideEffectCompleted(successAction)
                          case Failure(ex) =>
                            actorContext.self ! SideEffectFailure(ex, failureAction)
                        }
                    }
                } else {
                  Effect
                    .persist(state.withInnerState(result.state))
                    .thenRun((s: StateWrapper[State]) => s.waitingForProcessingCompletion = false)
                    .thenRun(_ => successAction())
                    .thenUnstashAll()
                }

              case AfterSideEffectCompleted(successAction) =>
                state.waitingForProcessingCompletion = false
                successAction()
                Effect.unstashAll()

              case SideEffectFailure(ex, failureAction) =>
                state.waitingForProcessingCompletion = false
                actorContext.log.error("Failure handling processing side effects", ex)
                failureAction(ex)
                Effect.none

              case flowInput: StatefulFlowHandler.ProcessFlowInput[In, Out, _] =>
                if (!state.waitingForProcessingCompletion) {
                  Try(logic.processInput(state.innerState, flowInput.in)) match {
                    case Success(result) =>
                      val successAction = () =>
                        flowInput.replyTo ! StatusReply.success(
                          result.outs -> flowInput.passthrough
                        )
                      val failureAction =
                        (ex: Throwable) => flowInput.replyTo ! StatusReply.error(ex)

                      handleResult(
                        actorContext.self,
                        state,
                        result,
                        successAction,
                        failureAction
                      )

                    case Failure(ex) =>
                      actorContext.log.error("Failure handling input", ex)
                      Effect.reply(flowInput.replyTo)(StatusReply.error(ex))
                  }
                } else Effect.stash()

              case commandRequest: StatefulFlowHandler.ProcessCommand[Command] =>
                if (!state.waitingForProcessingCompletion) {
                  Try(logic.processCommand(state.innerState, commandRequest.command)) match {
                    case Success(result) =>
                      val successAction = () => ()
                      val failureAction = (_: Throwable) => ()

                      handleResult(
                        actorContext.self,
                        state,
                        result,
                        successAction,
                        failureAction
                      )

                    case Failure(ex) =>
                      actorContext.log.error("Failure handling command", ex)
                      Effect.none
                  }
                } else Effect.stash()

              case StatefulFlowHandler.TerminateRequest(replyTo) =>
                if (!state.waitingForProcessingCompletion) {
                  replyTo.tell(StatusReply.ack())
                  Effect.stop()
                } else Effect.stash()

              case msg =>
                if (!state.waitingForProcessingCompletion) {
                  actorContext.log.error(
                    s"Inconsistency in AkkaPersistenceStatefulFlowBackend.DurableState. Received unexpected message: ${msg}"
                  )
                  Effect.stop()
                } else Effect.stash()
            }
        ).withDurableStateStorePluginId(storagePlugin.statePluginId)
          .receiveSignal {
            case (state, _: RecoveryCompleted) =>
              state.waitForRecoverCompletion = false
              actorContext.log.info(s"Recovery completed with state: ${state.innerState}")
            case (_, RecoveryFailed(ex)) => actorContext.log.error("Recovery failed", ex)
          }

        val behaviorWithSnapshotAdapter = snapshotAdapter
          .map(a => baseBehavior.snapshotAdapter(makeWrappedSnapshotAdapted(a)))
          .getOrElse(baseBehavior.snapshotAdapter(defaultSnapshotAdapter))

        behaviorWithSnapshotAdapter
      }
    }

    def apply[State](
        storagePlugin: DurableState.PersistencePlugin
      ): DurableState[State] =
      new DurableState(
        storagePlugin,
        1,
        None
      )
  }

  class DurableState[State](
      storagePlugin: DurableState.PersistencePlugin,
      sideEffectsParallelism: Int,
      snapshotAdapter: Option[SnapshotAdapter[State]])
      extends StatefulFlowBackend.DurableState[State, DurableState.AkkaPersistenceBackendProtocol] {
    override val id: String = "akka-persistence-durable-state"

    override def behaviorFor[In, Out, Command](
        logic: StatefulFlowLogic.DurableState[State, In, Out, Command],
        entityKind: String,
        entityId: String
      ): Behavior[StatefulFlowHandler.Protocol[In, Out, Command, DurableState.AkkaPersistenceBackendProtocol]] =
      DurableState.behaviorFactory(
        entityKind,
        entityId,
        logic,
        storagePlugin,
        sideEffectsParallelism,
        snapshotAdapter
      )

    def withSideEffectsParallelism(n: Int): DurableState[State] =
      new DurableState(
        storagePlugin,
        n,
        snapshotAdapter
      )

    def withSnapshotAdapter(
        snapshotAdapter: SnapshotAdapter[State]
      ): DurableState[State] =
      new DurableState[State](
        storagePlugin,
        sideEffectsParallelism,
        Some(snapshotAdapter)
      )

    def withSnapshotCodec(
        implicit snapshotCodec: Codec[State]
      ): DurableState[State] =
      new DurableState[State](
        storagePlugin,
        sideEffectsParallelism,
        Some(new SerializedDataSnapshotAdapter(snapshotCodec))
      )
  }
}
