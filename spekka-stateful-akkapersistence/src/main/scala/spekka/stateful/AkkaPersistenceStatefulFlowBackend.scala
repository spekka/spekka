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

/** An Akka Persistence based implementation of `StatefulFlowBackend`
  */
object AkkaPersistenceStatefulFlowBackend {

  /** Container of serialized data for `spekka.codec.Codec` based serialization
    *
    * @param bytes
    *   serialized bytes
    */
  class SerializedData(val bytes: Array[Byte])

  /** Akka serializer for [[SerializedData]]
    */
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

  private[AkkaPersistenceStatefulFlowBackend] class StateWrapper[State] private[StateWrapper] (
      var waitForRecoverCompletion: Boolean,
      var waitingForProcessingCompletion: Boolean,
      val innerState: State) {
    def withInnerState(s: State): StateWrapper[State] =
      new StateWrapper(waitForRecoverCompletion, waitingForProcessingCompletion, s)
  }

  private[AkkaPersistenceStatefulFlowBackend] object StateWrapper {
    def recoveredState[State](s: State): StateWrapper[State] = new StateWrapper(true, false, s)
  }

  /** Snapshot adapter for `spekka.codec.Codec` based serialization
    *
    * @param codec
    *   the codec to be used to serialize/deserialize the data
    */
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

  /** An implementation of `StatefulFlowBackend.EventBased` based on Akka Persistence Event
    * Sourcing.
    *
    * Events are stored in the journal and tagged with the following tag:
    * `$$entityKind-$$partitionId`. Partition ids are computed as follows:
    * `Math.abs(entityId.hashCode % eventPartitions)`.
    *
    * For further details on event tagging see the documentation of Akka Projections.
    */
  object EventBased {
    import akka.persistence.typed.scaladsl.Effect
    import akka.persistence.typed.scaladsl.EffectBuilder
    import akka.persistence.typed.RecoveryFailed
    import akka.persistence.typed.SnapshotCompleted
    import akka.persistence.typed.SnapshotFailed
    import akka.persistence.typed.RecoveryCompleted

    sealed private[spekka] trait AkkaPersistenceBackendProtocol
        extends StatefulFlowHandler.BackendProtocol[AkkaPersistenceBackendProtocol]

    private[spekka] case class InputProcessingResultReady[Ev](
        result: Try[StatefulFlowLogic.EventBased.ProcessingResult[Ev]],
        replyTo: ActorRef[StatusReply[StatefulFlowHandler.ProcessFlowOutput[Ev]]])
        extends StatefulFlowHandler.BackendProtocol[AkkaPersistenceBackendProtocol]

    private[spekka] case class CommandProcessingResultReady[Ev](
        result: Try[StatefulFlowLogic.EventBased.ProcessingResult[Ev]])
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

    /** Akka persistence plugins configuration
      */
    trait PersistencePlugin {

      /** Journal plugin
        */
      def journalPluginId: String

      /** Snapshot plugin
        */
      def snapshotPluginId: String
    }

    object PersistencePlugin {

      /** [[PersistencePlugin]] using Akka Persistence Cassandra
        */
      case object CassandraStoragePlugin extends PersistencePlugin {
        override val journalPluginId: String = "akka.persistence.cassandra.journal"
        override val snapshotPluginId: String = "akka.persistence.cassandra.snapshot"
      }

      /** [[PersistencePlugin]] using Akka Persistence JDBC
        */
      case object JdbcStoragePlugin extends PersistencePlugin {
        override val journalPluginId: String = "jdbc-journal"
        override val snapshotPluginId: String = "jdbc-snapshot-store"
      }

      /** [[PersistencePlugin]] allowing use of custom journal/snapshot Akka Persistence plugins
        *
        * @param journalPluginId
        *   the id of the journal plugin
        * @param snapshotPluginId
        *   the id of the snapshot plugin
        */
      case class CustomStoragePlugin(journalPluginId: String, snapshotPluginId: String)
          extends PersistencePlugin
    }

    /** Event adapter for `spekka.codec.Codec` based serialization
      *
      * @param codec
      *   the codec to be used to serialize/deserialize the data
      */
    class SerializedDataEventAdapter[T](codec: Codec[T]) extends EventAdapter[T, SerializedData] {
      override def manifest(event: T): String = ""
      override def toJournal(e: T): SerializedData = new SerializedData(codec.encoder.encode(e))

      override def fromJournal(p: SerializedData, manifest: String): EventSeq[T] =
        codec.decoder.decode(p.bytes).fold(throw _, e => EventSeq.single(e))
    }

    private[spekka] def handleResult[In, State, Ev, Command](
        self: ActorRef[
          StatefulFlowHandler.Protocol[In, Ev, Command, AkkaPersistenceBackendProtocol]
        ],
        state: StateWrapper[State],
        result: StatefulFlowLogic.EventBased.ProcessingResult[Ev],
        successAction: () => Unit,
        failureAction: (Throwable) => Unit,
        sideEffectsParallelism: Int
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

                case flowInput: StatefulFlowHandler.ProcessFlowInput[In, Ev] =>
                  if (!state.waitingForProcessingCompletion) {
                    Try(logic.processInput(state.innerState, flowInput.in)) match {
                      case Success(result) =>
                        val successAction = () =>
                          flowInput.replyTo ! StatusReply.success(
                            StatefulFlowHandler.ProcessFlowOutput(result.events)
                          )
                        val failureAction =
                          (ex: Throwable) => flowInput.replyTo ! StatusReply.error(ex)

                        handleResult(
                          actorContext.self,
                          state,
                          result,
                          successAction,
                          failureAction,
                          sideEffectsParallelism
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
                          failureAction,
                          sideEffectsParallelism
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

    /** Creates a new instance of [[AkkaPersistenceStatefulFlowBackend.EventBased]].
      *
      * @param storagePlugin
      *   the persistence plugin to use
      * @param retentionCriteria
      *   the retention criteria configuration
      * @param eventsPartitions
      *   the number of partitions to tag the events into (see Akka Projections)
      * @param sideEffectsParallelism
      *   the number of side effects to execute concurrently
      * @return
      *   [[EventBased]] instance
      * @tparam State
      *   state type handled by this backend
      * @tparam Ev
      *   events type handled by this backend
      */
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

  /** An `StatefulFlowBackend.EventBased` implementation based on Akka Persistence Event Sourcing.
    */
  class EventBased[State, Ev] private[spekka] (
      storagePlugin: EventBased.PersistencePlugin,
      retentionCriteria: RetentionCriteria,
      eventsPartitions: Int,
      sideEffectsParallelism: Int,
      eventAdapter: Option[EventAdapter[Ev, _]],
      snapshotAdapter: Option[SnapshotAdapter[State]])
      extends StatefulFlowBackend.EventBased[State, Ev, EventBased.AkkaPersistenceBackendProtocol] {
    override val id: String = "akka-persistence-event-based"

    override private[spekka] def behaviorFor[In, Command](
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

    /** Changes the number of partitions to be used for event tagging.
      *
      * Events are tagged with the following tag: `$$entityKind-$$partitionId`. Partition ids are
      * computed as follows: `Math.abs(entityId.hashCode % eventPartitions)`.
      * @param n
      *   the new value of event partitions
      * @return
      *   A new instance of [[EventBased]] with the specified event partitions
      */
    def withEventsPartitions(n: Int): EventBased[State, Ev] =
      new EventBased(
        storagePlugin,
        retentionCriteria,
        n,
        sideEffectsParallelism,
        eventAdapter,
        snapshotAdapter
      )

    /** Changes the side effect parallelism.
      *
      * @param n
      *   the new side effects parallelism
      * @return
      *   A new instance of [[EventBased]] with the specified side effect parallelism
      */
    def withSideEffectsParallelism(n: Int): EventBased[State, Ev] =
      new EventBased(
        storagePlugin,
        retentionCriteria,
        eventsPartitions,
        n,
        eventAdapter,
        snapshotAdapter
      )

    /** Allows the configuration of a custom event adapter.
      *
      * When using custom event adapters it is the responsibility of the programmer to correctly
      * configure Akka Serialization infrastructure.
      *
      * @param eventAdapter
      *   the event adapter to use
      * @return
      *   A new instance of [[EventBased]] with the specified event adapter
      */
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

    /** Allows the configuration of a custom snapshot adapter.
      *
      * When using custom snapshot adapters it is the responsibility of the programmer to correctly
      * configure Akka Serialization infrastructure.
      *
      * @param snapshotAdapter
      *   the snapshot adapter to use
      * @return
      *   A new instance of [[EventBased]] with the specified snapshot adapter
      */
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

    /** Configures an explicit event codec to be used during serialization.
      *
      * When using explicit codecs there is no need to configure Akka Serialization infrastructure.
      *
      * @param eventCodec
      *   the event codec to use
      * @return
      *   A new instance of [[EventBased]] with the specified event codec
      */
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

    /** Configures an explicit snapshot codec to be used during serialization.
      *
      * When using explicit codecs there is no need to configure Akka Serialization infrastructure.
      *
      * @param snapshotCodec
      *   the snapshot codec to use
      * @return
      *   A new instance of [[EventBased]] with the specified snapshot codec
      */
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

  object EventBasedAsync {
    import akka.persistence.typed.scaladsl.Effect
    import akka.persistence.typed.RecoveryFailed
    import akka.persistence.typed.SnapshotCompleted
    import akka.persistence.typed.SnapshotFailed
    import akka.persistence.typed.RecoveryCompleted

    private[spekka] def behaviorFactory[State, Ev, In, Command](
        entityKind: String,
        entityId: String,
        logic: StatefulFlowLogic.EventBasedAsync[State, Ev, In, Command],
        retentionCriteria: RetentionCriteria,
        storagePlugin: EventBased.PersistencePlugin,
        partitions: Int,
        sideEffectsParallelism: Int,
        eventAdapter: Option[EventAdapter[Ev, _]],
        snapshotAdapter: Option[SnapshotAdapter[State]]
      ): Behavior[StatefulFlowHandler.Protocol[In, Ev, Command, EventBased.AkkaPersistenceBackendProtocol]] = {
      val partitionId = Math.abs(entityId.hashCode % partitions)
      val partitionTag = Set(s"$entityKind-$partitionId")

      Behaviors.setup { actorContext =>
        implicit val ec: scala.concurrent.ExecutionContext = actorContext.executionContext

        val baseBehavior =
          EventSourcedBehavior[
            StatefulFlowHandler.Protocol[
              In,
              Ev,
              Command,
              EventBased.AkkaPersistenceBackendProtocol
            ],
            Ev,
            StateWrapper[State]
          ](
            PersistenceId(entityKind, entityId),
            StateWrapper.recoveredState(logic.initialState),
            (state, req) =>
              req match {
                case EventBased.BeforeSideEffectCompleted(
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
                              actorContext.self ! EventBased.AfterSideEffectCompleted(successAction)
                            case Failure(ex) =>
                              actorContext.self ! EventBased.SideEffectFailure(ex, failureAction)
                          }
                      }
                  } else {
                    Effect
                      .persist(result.events)
                      .thenRun((s: StateWrapper[State]) => s.waitingForProcessingCompletion = false)
                      .thenRun(_ => successAction())
                      .thenUnstashAll()
                  }

                case EventBased.AfterSideEffectCompleted(successAction) =>
                  state.waitingForProcessingCompletion = false
                  successAction()
                  Effect.unstashAll()

                case EventBased.SideEffectFailure(ex, failureAction) =>
                  state.waitingForProcessingCompletion = false
                  actorContext.log.error("Failure handling processing side effects", ex)
                  failureAction(ex)
                  Effect.none

                case flowInput: StatefulFlowHandler.ProcessFlowInput[In, Ev] =>
                  if (!state.waitingForProcessingCompletion) {
                    val self = actorContext.self

                    Try(logic.processInput(state.innerState, flowInput.in)) match {
                      case Success(resultF) =>
                        resultF.andThen(res =>
                          self.tell(
                            EventBased.InputProcessingResultReady(res, flowInput.replyTo)
                          )
                        )
                        Effect.none
                      case Failure(ex) =>
                        actorContext.log.error("Failure handling input", ex)
                        Effect.reply(flowInput.replyTo)(StatusReply.error(ex))
                        Effect.none
                    }
                  } else Effect.stash()

                case EventBased.InputProcessingResultReady(
                      res: Try[StatefulFlowLogic.EventBased.ProcessingResult[Ev]] @unchecked,
                      replyTo
                    ) =>
                  res match {
                    case Success(result) =>
                      val successAction = () => {
                        replyTo ! StatusReply.success(
                          StatefulFlowHandler.ProcessFlowOutput(result.events)
                        )
                      }
                      val failureAction =
                        (ex: Throwable) => replyTo ! StatusReply.error(ex)

                      EventBased.handleResult(
                        actorContext.self,
                        state,
                        result,
                        successAction,
                        failureAction,
                        sideEffectsParallelism
                      )

                    case Failure(ex) =>
                      actorContext.log.error("Failure handling input", ex)
                      Effect.reply(replyTo)(StatusReply.error(ex))

                  }

                case commandRequest: StatefulFlowHandler.ProcessCommand[Command] =>
                  if (!state.waitingForProcessingCompletion) {
                    val self = actorContext.self

                    Try(logic.processCommand(state.innerState, commandRequest.command)) match {
                      case Success(resultF) =>
                        resultF.andThen(res =>
                          self.tell(
                            EventBased.CommandProcessingResultReady(res)
                          )
                        )
                        Effect.none
                      case Failure(ex) =>
                        actorContext.log.error("Failure handling command", ex)
                        Effect.none
                    }
                  } else Effect.stash()

                case EventBased.CommandProcessingResultReady(
                      resultE: Try[StatefulFlowLogic.EventBased.ProcessingResult[Ev]] @unchecked
                    ) =>
                  resultE match {
                    case Success(result) =>
                      val successAction = () => ()
                      val failureAction = (_: Throwable) => ()

                      EventBased.handleResult(
                        actorContext.self,
                        state,
                        result,
                        successAction,
                        failureAction,
                        sideEffectsParallelism
                      )

                    case Failure(ex) =>
                      actorContext.log.error("Failure handling command", ex)
                      Effect.none
                  }

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

    /** Creates a new instance of [[AkkaPersistenceStatefulFlowBackend.EventBasedAsync]].
      *
      * @param storagePlugin
      *   the persistence plugin to use
      * @param retentionCriteria
      *   the retention criteria configuration
      * @param eventsPartitions
      *   the number of partitions to tag the events into (see Akka Projections)
      * @param sideEffectsParallelism
      *   the number of side effects to execute concurrently
      * @return
      *   [[EventBased]] instance
      * @tparam State
      *   state type handled by this backend
      * @tparam Ev
      *   events type handled by this backend
      */
    def apply[State, Ev](
        storagePlugin: EventBased.PersistencePlugin,
        retentionCriteria: RetentionCriteria = RetentionCriteria.disabled,
        eventsPartitions: Int = 1,
        sideEffectsParallelism: Int = 1
      ): EventBasedAsync[State, Ev] =
      new EventBasedAsync(
        storagePlugin,
        retentionCriteria,
        eventsPartitions,
        sideEffectsParallelism,
        None,
        None
      )
  }

  /** An `StatefulFlowBackend.EventBased` implementation based on Akka Persistence Event Sourcing.
    */
  class EventBasedAsync[State, Ev] private[spekka] (
      storagePlugin: EventBased.PersistencePlugin,
      retentionCriteria: RetentionCriteria,
      eventsPartitions: Int,
      sideEffectsParallelism: Int,
      eventAdapter: Option[EventAdapter[Ev, _]],
      snapshotAdapter: Option[SnapshotAdapter[State]])
      extends StatefulFlowBackend.EventBasedAsync[
        State,
        Ev,
        EventBased.AkkaPersistenceBackendProtocol
      ] {
    override val id: String = "akka-persistence-event-based-async"

    override private[spekka] def behaviorFor[In, Command](
        logic: StatefulFlowLogic.EventBasedAsync[State, Ev, In, Command],
        entityKind: String,
        entityId: String
      ): Behavior[StatefulFlowHandler.Protocol[In, Ev, Command, EventBased.AkkaPersistenceBackendProtocol]] =
      EventBasedAsync.behaviorFactory(
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

    /** Changes the number of partitions to be used for event tagging.
      *
      * Events are tagged with the following tag: `$$entityKind-$$partitionId`. Partition ids are
      * computed as follows: `Math.abs(entityId.hashCode % eventPartitions)`.
      * @param n
      *   the new value of event partitions
      * @return
      *   A new instance of [[EventBased]] with the specified event partitions
      */
    def withEventsPartitions(n: Int): EventBasedAsync[State, Ev] =
      new EventBasedAsync(
        storagePlugin,
        retentionCriteria,
        n,
        sideEffectsParallelism,
        eventAdapter,
        snapshotAdapter
      )

    /** Changes the side effect parallelism.
      *
      * @param n
      *   the new side effects parallelism
      * @return
      *   A new instance of [[EventBased]] with the specified side effect parallelism
      */
    def withSideEffectsParallelism(n: Int): EventBasedAsync[State, Ev] =
      new EventBasedAsync(
        storagePlugin,
        retentionCriteria,
        eventsPartitions,
        n,
        eventAdapter,
        snapshotAdapter
      )

    /** Allows the configuration of a custom event adapter.
      *
      * When using custom event adapters it is the responsibility of the programmer to correctly
      * configure Akka Serialization infrastructure.
      *
      * @param eventAdapter
      *   the event adapter to use
      * @return
      *   A new instance of [[EventBased]] with the specified event adapter
      */
    def withEventAdapter(
        eventAdapter: EventAdapter[Ev, _]
      ): EventBasedAsync[State, Ev] =
      new EventBasedAsync[State, Ev](
        storagePlugin,
        retentionCriteria,
        eventsPartitions,
        sideEffectsParallelism,
        Some(eventAdapter),
        snapshotAdapter
      )

    /** Allows the configuration of a custom snapshot adapter.
      *
      * When using custom snapshot adapters it is the responsibility of the programmer to correctly
      * configure Akka Serialization infrastructure.
      *
      * @param snapshotAdapter
      *   the snapshot adapter to use
      * @return
      *   A new instance of [[EventBased]] with the specified snapshot adapter
      */
    def withSnapshotAdapter(
        snapshotAdapter: SnapshotAdapter[State]
      ): EventBasedAsync[State, Ev] =
      new EventBasedAsync[State, Ev](
        storagePlugin,
        retentionCriteria,
        eventsPartitions,
        sideEffectsParallelism,
        eventAdapter,
        Some(snapshotAdapter)
      )

    /** Configures an explicit event codec to be used during serialization.
      *
      * When using explicit codecs there is no need to configure Akka Serialization infrastructure.
      *
      * @param eventCodec
      *   the event codec to use
      * @return
      *   A new instance of [[EventBased]] with the specified event codec
      */
    def withEventCodec(
        implicit eventCodec: Codec[Ev]
      ): EventBasedAsync[State, Ev] =
      new EventBasedAsync[State, Ev](
        storagePlugin,
        retentionCriteria,
        eventsPartitions,
        sideEffectsParallelism,
        Some(new EventBased.SerializedDataEventAdapter(eventCodec)),
        snapshotAdapter
      )

    /** Configures an explicit snapshot codec to be used during serialization.
      *
      * When using explicit codecs there is no need to configure Akka Serialization infrastructure.
      *
      * @param snapshotCodec
      *   the snapshot codec to use
      * @return
      *   A new instance of [[EventBased]] with the specified snapshot codec
      */
    def withSnapshotCodec(
        implicit snapshotCodec: Codec[State]
      ): EventBasedAsync[State, Ev] =
      new EventBasedAsync[State, Ev](
        storagePlugin,
        retentionCriteria,
        eventsPartitions,
        sideEffectsParallelism,
        eventAdapter,
        Some(new SerializedDataSnapshotAdapter(snapshotCodec))
      )
  }

  /** An implementation of `StatefulFlowBackend.DurableState` based on Akka Persistence Durable
    * State.
    */
  object DurableState {
    import akka.persistence.typed.state.scaladsl.Effect
    import akka.persistence.typed.state.scaladsl.EffectBuilder
    import akka.persistence.typed.state.RecoveryFailed
    import akka.persistence.typed.state.RecoveryCompleted

    sealed private[spekka] trait AkkaPersistenceBackendProtocol
        extends StatefulFlowHandler.BackendProtocol[AkkaPersistenceBackendProtocol]

    private[spekka] case class InputProcessingResultReady[State, Out](
        result: Try[StatefulFlowLogic.DurableState.ProcessingResult[State, Out]],
        replyTo: ActorRef[StatusReply[StatefulFlowHandler.ProcessFlowOutput[Out]]])
        extends StatefulFlowHandler.BackendProtocol[AkkaPersistenceBackendProtocol]

    private[spekka] case class CommandProcessingResultReady[State](
        result: Try[StatefulFlowLogic.DurableState.ProcessingResult[State, Nothing]])
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

    /** Akka persistence plugin configuration
      */
    trait PersistencePlugin {

      /** The state store plugin
        */
      def statePluginId: String
    }

    object PersistencePlugin {

      /** [[PersistencePlugin]] using Akka Persistence JDBC
        */
      case object JdbcStoragePlugin extends PersistencePlugin {
        override val statePluginId: String = "jdbc-durable-state-store"
      }

      /** [[PersistencePlugin]] allowing use of custom state Akka Persistence plugin
        *
        * @param statePluginId
        *   the id of the state plugin
        */
      case class CustomStoragePlugin(statePluginId: String) extends PersistencePlugin
    }

    private[spekka] def handleResult[State, In, Out, Command](
        self: ActorRef[
          StatefulFlowHandler.Protocol[In, Out, Command, AkkaPersistenceBackendProtocol]
        ],
        state: StateWrapper[State],
        result: StatefulFlowLogic.DurableState.ProcessingResult[State, _],
        successAction: () => Unit,
        failureAction: (Throwable) => Unit,
        sideEffectsParallelism: Int
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

    private[spekka] def behaviorFactory[State, In, Out, Command](
        entityKind: String,
        entityId: String,
        logic: StatefulFlowLogic.DurableState[State, In, Out, Command],
        storagePlugin: PersistencePlugin,
        sideEffectsParallelism: Int,
        snapshotAdapter: Option[SnapshotAdapter[State]]
      ): Behavior[StatefulFlowHandler.Protocol[In, Out, Command, AkkaPersistenceBackendProtocol]] = {
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

              case flowInput: StatefulFlowHandler.ProcessFlowInput[In, Out] =>
                if (!state.waitingForProcessingCompletion) {
                  Try(logic.processInput(state.innerState, flowInput.in)) match {
                    case Success(result) =>
                      val successAction = () =>
                        flowInput.replyTo ! StatusReply.success(
                          StatefulFlowHandler.ProcessFlowOutput(result.outs)
                        )
                      val failureAction =
                        (ex: Throwable) => flowInput.replyTo ! StatusReply.error(ex)

                      handleResult(
                        actorContext.self,
                        state,
                        result,
                        successAction,
                        failureAction,
                        sideEffectsParallelism
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
                        failureAction,
                        sideEffectsParallelism
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

    /** Creates a new instance of [[AkkaPersistenceStatefulFlowBackend.DurableState]].
      *
      * @param storagePlugin
      *   the persistence plugin to use
      * @param sideEffectsParallelism
      *   the number of side effects to execute concurrently
      * @return
      *   [[DurableState]] instance
      * @tparam State
      *   state type handled by this backend
      */
    def apply[State](
        storagePlugin: DurableState.PersistencePlugin,
        sideEffectsParallelism: Int = 1
      ): DurableState[State] =
      new DurableState(
        storagePlugin,
        sideEffectsParallelism,
        None
      )
  }

  /** An `StatefulFlowBackend.DurableState` implementation based on Akka Persistence Durable State.
    */
  class DurableState[State] private[spekka] (
      storagePlugin: DurableState.PersistencePlugin,
      sideEffectsParallelism: Int,
      snapshotAdapter: Option[SnapshotAdapter[State]])
      extends StatefulFlowBackend.DurableState[State, DurableState.AkkaPersistenceBackendProtocol] {
    override val id: String = "akka-persistence-durable-state"

    override private[spekka] def behaviorFor[In, Out, Command](
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

    /** Changes the side effect parallelism.
      *
      * @param n
      *   the new side effects parallelism
      * @return
      *   A new instance of [[DurableState]] with the specified side effect parallelism
      */
    def withSideEffectsParallelism(n: Int): DurableState[State] =
      new DurableState(
        storagePlugin,
        n,
        snapshotAdapter
      )

    /** Allows the configuration of a custom snapshot adapter.
      *
      * When using custom snapshot adapters it is the responsibility of the programmer to correctly
      * configure Akka Serialization infrastructure.
      *
      * @param snapshotAdapter
      *   the snapshot adapter to use
      * @return
      *   A new instance of [[DurableState]] with the specified snapshot adapter
      */
    def withSnapshotAdapter(
        snapshotAdapter: SnapshotAdapter[State]
      ): DurableState[State] =
      new DurableState[State](
        storagePlugin,
        sideEffectsParallelism,
        Some(snapshotAdapter)
      )

    /** Configures an explicit snapshot codec to be used during serialization.
      *
      * When using explicit codecs there is no need to configure Akka Serialization infrastructure.
      *
      * @param snapshotCodec
      *   the snapshot codec to use
      * @return
      *   A new instance of [[DurableState]] with the specified snapshot codec
      */
    def withSnapshotCodec(
        implicit snapshotCodec: Codec[State]
      ): DurableState[State] =
      new DurableState[State](
        storagePlugin,
        sideEffectsParallelism,
        Some(new SerializedDataSnapshotAdapter(snapshotCodec))
      )
  }

  object DurableStateAsync {
    import akka.persistence.typed.state.scaladsl.Effect
    import akka.persistence.typed.state.RecoveryFailed
    import akka.persistence.typed.state.RecoveryCompleted

    private[spekka] def behaviorFactory[State, In, Out, Command](
        entityKind: String,
        entityId: String,
        logic: StatefulFlowLogic.DurableStateAsync[State, In, Out, Command],
        storagePlugin: DurableState.PersistencePlugin,
        sideEffectsParallelism: Int,
        snapshotAdapter: Option[SnapshotAdapter[State]]
      ): Behavior[StatefulFlowHandler.Protocol[In, Out, Command, DurableState.AkkaPersistenceBackendProtocol]] = {
      Behaviors.setup { actorContext =>
        implicit val ec: scala.concurrent.ExecutionContext = actorContext.executionContext

        val baseBehavior = DurableStateBehavior[
          StatefulFlowHandler.Protocol[
            In,
            Out,
            Command,
            DurableState.AkkaPersistenceBackendProtocol
          ],
          StateWrapper[State]
        ](
          PersistenceId(entityKind, entityId),
          StateWrapper.recoveredState(logic.initialState),
          (state, req) =>
            req match {
              case DurableState.BeforeSideEffectCompleted(
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
                            actorContext.self ! DurableState.AfterSideEffectCompleted(successAction)
                          case Failure(ex) =>
                            actorContext.self ! DurableState.SideEffectFailure(ex, failureAction)
                        }
                    }
                } else {
                  Effect
                    .persist(state.withInnerState(result.state))
                    .thenRun((s: StateWrapper[State]) => s.waitingForProcessingCompletion = false)
                    .thenRun(_ => successAction())
                    .thenUnstashAll()
                }

              case DurableState.AfterSideEffectCompleted(successAction) =>
                state.waitingForProcessingCompletion = false
                successAction()
                Effect.unstashAll()

              case DurableState.SideEffectFailure(ex, failureAction) =>
                state.waitingForProcessingCompletion = false
                actorContext.log.error("Failure handling processing side effects", ex)
                failureAction(ex)
                Effect.none

              case flowInput: StatefulFlowHandler.ProcessFlowInput[In, Out] =>
                if (!state.waitingForProcessingCompletion) {
                  val self = actorContext.self

                  Try(logic.processInput(state.innerState, flowInput.in)) match {
                    case Success(resultF) =>
                      resultF.andThen(res =>
                        self.tell(
                          DurableState.InputProcessingResultReady(res, flowInput.replyTo)
                        )
                      )
                      Effect.none

                    case Failure(ex) =>
                      actorContext.log.error("Failure handling input", ex)
                      Effect.reply(flowInput.replyTo)(StatusReply.error(ex))
                  }
                } else Effect.stash()

              case DurableState.InputProcessingResultReady(
                    res: Try[
                      StatefulFlowLogic.DurableState.ProcessingResult[State, Out]
                    ] @unchecked,
                    replyTo
                  ) =>
                res match {
                  case Success(result) =>
                    val successAction = () =>
                      replyTo ! StatusReply.success(
                        StatefulFlowHandler.ProcessFlowOutput(result.outs)
                      )
                    val failureAction =
                      (ex: Throwable) => replyTo ! StatusReply.error(ex)

                    DurableState.handleResult(
                      actorContext.self,
                      state,
                      result,
                      successAction,
                      failureAction,
                      sideEffectsParallelism
                    )

                  case Failure(ex) =>
                    actorContext.log.error("Failure handling input", ex)
                    Effect.reply(replyTo)(StatusReply.error(ex))

                }

              case commandRequest: StatefulFlowHandler.ProcessCommand[Command] =>
                if (!state.waitingForProcessingCompletion) {
                  val self = actorContext.self

                  Try(logic.processCommand(state.innerState, commandRequest.command)) match {
                    case Success(resultF) =>
                      resultF.andThen(res =>
                        self.tell(
                          DurableState.CommandProcessingResultReady(res)
                        )
                      )
                      Effect.none

                    case Failure(ex) =>
                      actorContext.log.error("Failure handling command", ex)
                      Effect.none
                  }
                } else Effect.stash()

              case DurableState.CommandProcessingResultReady(
                    resultE: Try[
                      StatefulFlowLogic.DurableState.ProcessingResult[State, Out]
                    ] @unchecked
                  ) =>
                resultE match {
                  case Success(result) =>
                    val successAction = () => ()
                    val failureAction = (_: Throwable) => ()

                    DurableState.handleResult(
                      actorContext.self,
                      state,
                      result,
                      successAction,
                      failureAction,
                      sideEffectsParallelism
                    )

                  case Failure(ex) =>
                    actorContext.log.error("Failure handling command", ex)
                    Effect.none
                }

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

    /** Creates a new instance of [[AkkaPersistenceStatefulFlowBackend.DurableStateAsync]].
      *
      * @param storagePlugin
      *   the persistence plugin to use
      * @param sideEffectsParallelism
      *   the number of side effects to execute concurrently
      * @return
      *   [[DurableState]] instance
      * @tparam State
      *   state type handled by this backend
      */
    def apply[State](
        storagePlugin: DurableState.PersistencePlugin,
        sideEffectsParallelism: Int = 1
      ): DurableStateAsync[State] =
      new DurableStateAsync(
        storagePlugin,
        sideEffectsParallelism,
        None
      )
  }

  /** An `StatefulFlowBackend.DurableState` implementation based on Akka Persistence Durable State.
    */
  class DurableStateAsync[State] private[spekka] (
      storagePlugin: DurableState.PersistencePlugin,
      sideEffectsParallelism: Int,
      snapshotAdapter: Option[SnapshotAdapter[State]])
      extends StatefulFlowBackend.DurableStateAsync[
        State,
        DurableState.AkkaPersistenceBackendProtocol
      ] {
    override val id: String = "akka-persistence-durable-state-async"

    override private[spekka] def behaviorFor[In, Out, Command](
        logic: StatefulFlowLogic.DurableStateAsync[State, In, Out, Command],
        entityKind: String,
        entityId: String
      ): Behavior[StatefulFlowHandler.Protocol[In, Out, Command, DurableState.AkkaPersistenceBackendProtocol]] =
      DurableStateAsync.behaviorFactory(
        entityKind,
        entityId,
        logic,
        storagePlugin,
        sideEffectsParallelism,
        snapshotAdapter
      )

    /** Changes the side effect parallelism.
      *
      * @param n
      *   the new side effects parallelism
      * @return
      *   A new instance of [[DurableState]] with the specified side effect parallelism
      */
    def withSideEffectsParallelism(n: Int): DurableStateAsync[State] =
      new DurableStateAsync(
        storagePlugin,
        n,
        snapshotAdapter
      )

    /** Allows the configuration of a custom snapshot adapter.
      *
      * When using custom snapshot adapters it is the responsibility of the programmer to correctly
      * configure Akka Serialization infrastructure.
      *
      * @param snapshotAdapter
      *   the snapshot adapter to use
      * @return
      *   A new instance of [[DurableState]] with the specified snapshot adapter
      */
    def withSnapshotAdapter(
        snapshotAdapter: SnapshotAdapter[State]
      ): DurableStateAsync[State] =
      new DurableStateAsync[State](
        storagePlugin,
        sideEffectsParallelism,
        Some(snapshotAdapter)
      )

    /** Configures an explicit snapshot codec to be used during serialization.
      *
      * When using explicit codecs there is no need to configure Akka Serialization infrastructure.
      *
      * @param snapshotCodec
      *   the snapshot codec to use
      * @return
      *   A new instance of [[DurableState]] with the specified snapshot codec
      */
    def withSnapshotCodec(
        implicit snapshotCodec: Codec[State]
      ): DurableStateAsync[State] =
      new DurableStateAsync[State](
        storagePlugin,
        sideEffectsParallelism,
        Some(new SerializedDataSnapshotAdapter(snapshotCodec))
      )
  }
}
