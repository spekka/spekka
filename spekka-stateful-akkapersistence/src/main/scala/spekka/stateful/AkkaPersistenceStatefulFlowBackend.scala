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
import akka.actor.typed.scaladsl.ActorContext
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
import org.slf4j.Logger
import spekka.codec.Codec

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
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
      val innerState: State
    ) {
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
        replyTo: ActorRef[StatusReply[StatefulFlowHandler.ProcessFlowOutput[Ev]]]
      ) extends StatefulFlowHandler.BackendProtocol[AkkaPersistenceBackendProtocol]

    private[spekka] case class CommandProcessingResultReady[Ev](
        result: Try[StatefulFlowLogic.EventBased.ProcessingResult[Ev]]
      ) extends StatefulFlowHandler.BackendProtocol[AkkaPersistenceBackendProtocol]

    private[spekka] case class BeforeSideEffectCompleted[Ev](
        result: StatefulFlowLogic.EventBased.ProcessingResult[Ev],
        successAction: () => Unit,
        failureAction: (Throwable) => Unit
      ) extends AkkaPersistenceBackendProtocol

    private[spekka] case class AfterSideEffectCompleted(
        successAction: () => Unit
      ) extends AkkaPersistenceBackendProtocol

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
        state.waitingForProcessingCompletion = false
        Effect
          .persist(result.events)
          .thenRun((_: StateWrapper[State]) => successAction())
      }
    }

    private[spekka] def handleBeforeSideEffect[State, Ev, In, Command](
        result: StatefulFlowLogic.EventBased.ProcessingResult[Ev @unchecked],
        successAction: () => Unit,
        failureAction: (Throwable) => Unit,
        self: ActorRef[
          StatefulFlowHandler.Protocol[In, Ev, Command, AkkaPersistenceBackendProtocol]
        ],
        sideEffectsParallelism: Int
      )(implicit ec: ExecutionContext
      ): Effect[Ev, StateWrapper[State]] = {
      if (result.afterUpdateSideEffects.nonEmpty) {
        Effect
          .persist(result.events)
          .thenRun { _: StateWrapper[State] =>
            SideEffectRunner
              .run(result.afterUpdateSideEffects, sideEffectsParallelism)
              .onComplete {
                case Success(_) =>
                  self ! AfterSideEffectCompleted(successAction)
                case Failure(ex) =>
                  self ! SideEffectFailure(ex, failureAction)
              }
          }
      } else {
        Effect
          .persist(result.events)
          .thenRun((s: StateWrapper[State]) => s.waitingForProcessingCompletion = false)
          .thenRun(_ => successAction())
          .thenUnstashAll()
      }
    }

    private[spekka] def handleAfterSideEffect[State](
        state: StateWrapper[State],
        successAction: () => Unit
      ): Effect[Nothing, StateWrapper[State]] = {
      state.waitingForProcessingCompletion = false
      successAction()
      Effect.unstashAll[Nothing, StateWrapper[State]]()
    }

    private[spekka] def handleSideEffectFailure[State](
        state: StateWrapper[State],
        failureAction: (Throwable) => Unit,
        ex: Throwable,
        log: Logger
      ): EffectBuilder[Nothing, StateWrapper[State]] = {
      state.waitingForProcessingCompletion = false
      log.error("Failure handling processing side effects", ex)
      failureAction(ex)
      Effect.none[Nothing, StateWrapper[State]]
    }

    private[spekka] def handleInputImpl[State, Ev, In, Command](
        state: StateWrapper[State],
        result: Try[StatefulFlowLogic.EventBased.ProcessingResult[Ev]],
        actorContext: ActorContext[
          StatefulFlowHandler.Protocol[In, Ev, Command, AkkaPersistenceBackendProtocol]
        ],
        replyTo: ActorRef[
          StatusReply[StatefulFlowHandler.ProcessFlowOutput[Ev]]
        ],
        sideEffectsParallelism: Int
      ): Effect[Ev, StateWrapper[State]] = {
      result match {
        case Success(result) =>
          val successAction = () =>
            replyTo ! StatusReply.success(
              StatefulFlowHandler.ProcessFlowOutput(result.events)
            )
          val failureAction =
            (ex: Throwable) => replyTo ! StatusReply.error(ex)

          handleResult(
            actorContext.self,
            state,
            result,
            successAction,
            failureAction,
            sideEffectsParallelism
          )(actorContext.executionContext)
        case Failure(ex) =>
          actorContext.log.error("Failure handling input", ex)
          Effect.reply(replyTo)(StatusReply.error(ex))

      }
    }

    private[spekka] def handleInputSync[State, Ev, In, Command](
        state: StateWrapper[State],
        flowInput: StatefulFlowHandler.ProcessFlowInput[In, Ev],
        logic: StatefulFlowLogic.EventBased[State, Ev, In, Command],
        actorContext: ActorContext[
          StatefulFlowHandler.Protocol[In, Ev, Command, AkkaPersistenceBackendProtocol]
        ],
        sideEffectsParallelism: Int
      ): Effect[Ev, StateWrapper[State]] = {
      if (!state.waitingForProcessingCompletion) {
        val result = Try(logic.processInput(state.innerState, flowInput.in))
        handleInputImpl(state, result, actorContext, flowInput.replyTo, sideEffectsParallelism)
      } else Effect.stash()
    }

    private[spekka] def handleInputAsync[State, Ev, In, Command](
        state: StateWrapper[State],
        flowInput: StatefulFlowHandler.ProcessFlowInput[In, Ev],
        logic: StatefulFlowLogic.EventBasedAsync[State, Ev, In, Command],
        actorContext: ActorContext[
          StatefulFlowHandler.Protocol[In, Ev, Command, AkkaPersistenceBackendProtocol]
        ],
        sideEffectsParallelism: Int
      ): Effect[Ev, StateWrapper[State]] = {
      if (!state.waitingForProcessingCompletion) {
        Try(logic.processInput(state.innerState, flowInput.in)) match {
          case Success(resultF) =>
            resultF.value match {
              case Some(result) =>
                handleInputImpl(
                  state,
                  result,
                  actorContext,
                  flowInput.replyTo,
                  sideEffectsParallelism
                )

              case None =>
                state.waitingForProcessingCompletion = true
                resultF.andThen { case res =>
                  actorContext.self.tell(
                    EventBased.InputProcessingResultReady(res, flowInput.replyTo)
                  )
                }(actorContext.executionContext)
                Effect.none
            }

          case Failure(ex) =>
            actorContext.log.error("Failure handling input", ex)
            Effect.reply(flowInput.replyTo)(StatusReply.error(ex))
            Effect.none
        }
      } else Effect.stash()
    }

    private[spekka] def handleCommandImpl[State, Ev, In, Command](
        state: StateWrapper[State],
        result: Try[StatefulFlowLogic.EventBased.ProcessingResult[Ev]],
        actorContext: ActorContext[
          StatefulFlowHandler.Protocol[In, Ev, Command, AkkaPersistenceBackendProtocol]
        ],
        sideEffectsParallelism: Int
      ): Effect[Ev, StateWrapper[State]] = {
      result match {
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
          )(actorContext.executionContext)

        case Failure(ex) =>
          actorContext.log.error("Failure handling command", ex)
          Effect.none
      }
    }

    private[spekka] def handleCommandSync[State, Ev, In, Command](
        state: StateWrapper[State],
        commandRequest: StatefulFlowHandler.ProcessCommand[Command],
        logic: StatefulFlowLogic.EventBased[State, Ev, In, Command],
        actorContext: ActorContext[
          StatefulFlowHandler.Protocol[In, Ev, Command, AkkaPersistenceBackendProtocol]
        ],
        sideEffectsParallelism: Int
      ): Effect[Ev, StateWrapper[State]] = {
      if (!state.waitingForProcessingCompletion) {
        handleCommandImpl(
          state,
          Try(logic.processCommand(state.innerState, commandRequest.command)),
          actorContext,
          sideEffectsParallelism
        )
      } else Effect.stash()
    }

    private[spekka] def handleCommandAsync[State, Ev, In, Command](
        state: StateWrapper[State],
        commandRequest: StatefulFlowHandler.ProcessCommand[Command],
        logic: StatefulFlowLogic.EventBasedAsync[State, Ev, In, Command],
        actorContext: ActorContext[
          StatefulFlowHandler.Protocol[In, Ev, Command, AkkaPersistenceBackendProtocol]
        ],
        sideEffectsParallelism: Int
      ): Effect[Ev, StateWrapper[State]] = {
      if (!state.waitingForProcessingCompletion) {
        Try(logic.processCommand(state.innerState, commandRequest.command)) match {
          case Success(resultF) =>
            resultF.value match {
              case Some(result) =>
                handleCommandImpl(state, result, actorContext, sideEffectsParallelism)

              case None =>
                state.waitingForProcessingCompletion = true
                resultF.andThen { case res =>
                  actorContext.self.tell(
                    EventBased.CommandProcessingResultReady(res)
                  )
                }(actorContext.executionContext)
                Effect.none
            }

          case Failure(ex) =>
            actorContext.log.error("Failure handling command", ex)
            Effect.none
        }
      } else Effect.stash()
    }

    private[spekka] def behaviorFactoryProto[State, Ev, In, Command](
        entityKind: String,
        entityId: String,
        initialState: () => State,
        updateState: (State, Ev) => State,
        retentionCriteria: RetentionCriteria,
        storagePlugin: PersistencePlugin,
        partitions: Int,
        sideEffectsParallelism: Int,
        eventAdapter: Option[EventAdapter[Ev, _]],
        snapshotAdapter: Option[SnapshotAdapter[State]],
        inputHandler: (
            StateWrapper[State],
            StatefulFlowHandler.ProcessFlowInput[In, Ev],
            ActorContext[
              StatefulFlowHandler.Protocol[In, Ev, Command, AkkaPersistenceBackendProtocol]
            ]
          ) => Effect[Ev, StateWrapper[State]],
        inputProcessingReadyHandler: (
            StateWrapper[State],
            InputProcessingResultReady[Ev],
            ActorContext[
              StatefulFlowHandler.Protocol[In, Ev, Command, AkkaPersistenceBackendProtocol]
            ]
          ) => Effect[Ev, StateWrapper[State]],
        commandHandler: (
            StateWrapper[State],
            StatefulFlowHandler.ProcessCommand[Command],
            ActorContext[
              StatefulFlowHandler.Protocol[In, Ev, Command, AkkaPersistenceBackendProtocol]
            ]
          ) => Effect[Ev, StateWrapper[State]],
        commandProcessingReadyHandler: (
            StateWrapper[State],
            CommandProcessingResultReady[Ev],
            ActorContext[
              StatefulFlowHandler.Protocol[In, Ev, Command, AkkaPersistenceBackendProtocol]
            ]
          ) => Effect[Ev, StateWrapper[State]]
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
            StateWrapper.recoveredState(initialState()),
            (state, req) =>
              req match {
                case BeforeSideEffectCompleted(
                      result: StatefulFlowLogic.EventBased.ProcessingResult[Ev @unchecked],
                      successAction,
                      failureAction
                    ) =>
                  handleBeforeSideEffect(
                    result,
                    successAction,
                    failureAction,
                    actorContext.self,
                    sideEffectsParallelism
                  )

                case AfterSideEffectCompleted(successAction) =>
                  handleAfterSideEffect(state, successAction)

                case SideEffectFailure(ex, failureAction) =>
                  handleSideEffectFailure(state, failureAction, ex, actorContext.log)

                case flowInput: StatefulFlowHandler.ProcessFlowInput[In, Ev] =>
                  inputHandler(
                    state,
                    flowInput,
                    actorContext
                  )

                case msg: InputProcessingResultReady[Ev @unchecked] =>
                  inputProcessingReadyHandler(
                    state,
                    msg,
                    actorContext
                  )

                case commandRequest: StatefulFlowHandler.ProcessCommand[Command] =>
                  commandHandler(
                    state,
                    commandRequest,
                    actorContext
                  )

                case msg: CommandProcessingResultReady[Ev @unchecked] =>
                  commandProcessingReadyHandler(
                    state,
                    msg,
                    actorContext
                  )

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
            (state, event) => state.withInnerState(updateState(state.innerState, event))
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
      behaviorFactoryProto[State, Ev, In, Command](
        entityKind,
        entityId,
        () => logic.initialState(entityKind, entityId),
        logic.updateState,
        retentionCriteria,
        storagePlugin,
        partitions,
        sideEffectsParallelism,
        eventAdapter,
        snapshotAdapter,
        (state, input, actorContext) =>
          handleInputSync(state, input, logic, actorContext, sideEffectsParallelism),
        (_, _, _) =>
          throw new IllegalStateException(
            "Inconsistency in AkkaPersistenceStatefulFlowBackend.EventBased"
          ),
        (state, command, actorContext) =>
          handleCommandSync(state, command, logic, actorContext, sideEffectsParallelism),
        (_, _, _) =>
          throw new IllegalStateException(
            "Inconsistency in AkkaPersistenceStatefulFlowBackend.EventBased"
          )
      )
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
      snapshotAdapter: Option[SnapshotAdapter[State]]
    ) extends StatefulFlowBackend.EventBased[State, Ev, EventBased.AkkaPersistenceBackendProtocol] {
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

  /** An implementation of `StatefulFlowBackend.EventBasedAsync` based on Akka Persistence Event
    * Sourcing.
    *
    * Events are stored in the journal and tagged with the following tag:
    * `$$entityKind-$$partitionId`. Partition ids are computed as follows:
    * `Math.abs(entityId.hashCode % eventPartitions)`.
    *
    * For further details on event tagging see the documentation of Akka Projections.
    */
  object EventBasedAsync {
    private[spekka] def behaviorFactory[State, Ev, In, Command](
        entityKind: String,
        entityId: String,
        logic: StatefulFlowLogic.EventBasedAsync[State, Ev, In, Command],
        retentionCriteria: RetentionCriteria,
        storagePlugin: EventBased.PersistencePlugin,
        partitions: Int,
        sideEffectsParallelism: Int,
        eventAdapter: Option[EventAdapter[Ev, _]],
        snapshotAdapter: Option[SnapshotAdapter[State]],
        initializationTimeout: FiniteDuration
      ): Behavior[StatefulFlowHandler.Protocol[In, Ev, Command, EventBased.AkkaPersistenceBackendProtocol]] = {
      StatefulFlowBackend.FutureBehavior(
        initializationTimeout,
        logic.initialState(entityKind, entityId)
      ) { state =>
        EventBased.behaviorFactoryProto[State, Ev, In, Command](
          entityKind,
          entityId,
          () => state,
          logic.updateState,
          retentionCriteria,
          storagePlugin,
          partitions,
          sideEffectsParallelism,
          eventAdapter,
          snapshotAdapter,
          (state, input, actorContext) =>
            EventBased.handleInputAsync(state, input, logic, actorContext, sideEffectsParallelism),
          (state, msg, actorContext) =>
            EventBased
              .handleInputImpl(
                state,
                msg.result,
                actorContext,
                msg.replyTo,
                sideEffectsParallelism
              ),
          (state, command, actorContext) =>
            EventBased
              .handleCommandAsync(state, command, logic, actorContext, sideEffectsParallelism),
          (state, msg, actorContext) =>
            EventBased.handleCommandImpl(state, msg.result, actorContext, sideEffectsParallelism)
        )
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
      *   [[EventBasedAsync]] instance
      * @tparam State
      *   state type handled by this backend
      * @tparam Ev
      *   events type handled by this backend
      */
    def apply[State, Ev](
        storagePlugin: EventBased.PersistencePlugin,
        retentionCriteria: RetentionCriteria = RetentionCriteria.disabled,
        eventsPartitions: Int = 1,
        sideEffectsParallelism: Int = 1,
        initializationTimeout: FiniteDuration = 30.seconds
      ): EventBasedAsync[State, Ev] =
      new EventBasedAsync(
        storagePlugin,
        retentionCriteria,
        eventsPartitions,
        sideEffectsParallelism,
        None,
        None,
        initializationTimeout
      )
  }

  /** An `StatefulFlowBackend.EventBasedAsync` implementation based on Akka Persistence Event
    * Sourcing.
    */
  class EventBasedAsync[State, Ev] private[spekka] (
      storagePlugin: EventBased.PersistencePlugin,
      retentionCriteria: RetentionCriteria,
      eventsPartitions: Int,
      sideEffectsParallelism: Int,
      eventAdapter: Option[EventAdapter[Ev, _]],
      snapshotAdapter: Option[SnapshotAdapter[State]],
      initializationTimeout: FiniteDuration
    ) extends StatefulFlowBackend.EventBasedAsync[
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
        snapshotAdapter,
        initializationTimeout
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
        snapshotAdapter,
        initializationTimeout
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
        snapshotAdapter,
        initializationTimeout
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
        snapshotAdapter,
        initializationTimeout
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
        Some(snapshotAdapter),
        initializationTimeout
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
        snapshotAdapter,
        initializationTimeout
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
        Some(new SerializedDataSnapshotAdapter(snapshotCodec)),
        initializationTimeout
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
        replyTo: ActorRef[StatusReply[StatefulFlowHandler.ProcessFlowOutput[Out]]]
      ) extends StatefulFlowHandler.BackendProtocol[AkkaPersistenceBackendProtocol]

    private[spekka] case class CommandProcessingResultReady[State](
        result: Try[StatefulFlowLogic.DurableState.ProcessingResult[State, Nothing]]
      ) extends StatefulFlowHandler.BackendProtocol[AkkaPersistenceBackendProtocol]

    private[spekka] case class BeforeSideEffectCompleted[State, Out](
        result: StatefulFlowLogic.DurableState.ProcessingResult[State, Out],
        successAction: () => Unit,
        failureAction: (Throwable) => Unit
      ) extends AkkaPersistenceBackendProtocol

    private[spekka] case class AfterSideEffectCompleted(
        successAction: () => Unit
      ) extends AkkaPersistenceBackendProtocol

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
        state.waitingForProcessingCompletion = false
        Effect
          .persist(state.withInnerState(result.state))
          .thenRun((_: StateWrapper[State]) => successAction())
      }
    }

    private[spekka] def handleBeforeSideEffect[State, Ev, In, Command](
        state: StateWrapper[State],
        result: StatefulFlowLogic.DurableState.ProcessingResult[State, _],
        successAction: () => Unit,
        failureAction: (Throwable) => Unit,
        self: ActorRef[
          StatefulFlowHandler.Protocol[In, Ev, Command, AkkaPersistenceBackendProtocol]
        ],
        sideEffectsParallelism: Int
      )(implicit ec: ExecutionContext
      ): Effect[StateWrapper[State]] = {
      if (result.afterUpdateSideEffects.nonEmpty) {
        Effect
          .persist(state.withInnerState(result.state))
          .thenRun { _ =>
            SideEffectRunner
              .run(result.afterUpdateSideEffects, sideEffectsParallelism)
              .onComplete {
                case Success(_) =>
                  self ! AfterSideEffectCompleted(successAction)
                case Failure(ex) =>
                  self ! SideEffectFailure(ex, failureAction)
              }
          }
      } else {
        Effect
          .persist(state.withInnerState(result.state))
          .thenRun((s: StateWrapper[State]) => s.waitingForProcessingCompletion = false)
          .thenRun(_ => successAction())
          .thenUnstashAll()
      }
    }

    private[spekka] def handleAfterSideEffect[State](
        state: StateWrapper[State],
        successAction: () => Unit
      ): Effect[StateWrapper[State]] = {
      state.waitingForProcessingCompletion = false
      successAction()
      Effect.unstashAll[StateWrapper[State]]()
    }

    private[spekka] def handleSideEffectFailure[State](
        state: StateWrapper[State],
        failureAction: (Throwable) => Unit,
        ex: Throwable,
        log: Logger
      ): EffectBuilder[StateWrapper[State]] = {
      state.waitingForProcessingCompletion = false
      log.error("Failure handling processing side effects", ex)
      failureAction(ex)
      Effect.none[StateWrapper[State]]
    }

    private[spekka] def handleInputImpl[State, In, Out, Command](
        state: StateWrapper[State],
        result: Try[StatefulFlowLogic.DurableState.ProcessingResult[State, Out]],
        actorContext: ActorContext[
          StatefulFlowHandler.Protocol[In, Out, Command, AkkaPersistenceBackendProtocol]
        ],
        replyTo: ActorRef[
          StatusReply[StatefulFlowHandler.ProcessFlowOutput[Out]]
        ],
        sideEffectsParallelism: Int
      ): Effect[StateWrapper[State]] = {
      result match {
        case Success(result) =>
          val successAction = () =>
            replyTo ! StatusReply.success(
              StatefulFlowHandler.ProcessFlowOutput(result.outs)
            )
          val failureAction =
            (ex: Throwable) => replyTo ! StatusReply.error(ex)

          handleResult(
            actorContext.self,
            state,
            result,
            successAction,
            failureAction,
            sideEffectsParallelism
          )(actorContext.executionContext)

        case Failure(ex) =>
          actorContext.log.error("Failure handling input", ex)
          Effect.reply(replyTo)(StatusReply.error(ex))
      }
    }

    private[spekka] def handleInputSync[State, In, Out, Command](
        state: StateWrapper[State],
        flowInput: StatefulFlowHandler.ProcessFlowInput[In, Out],
        logic: StatefulFlowLogic.DurableState[State, In, Out, Command],
        actorContext: ActorContext[
          StatefulFlowHandler.Protocol[In, Out, Command, AkkaPersistenceBackendProtocol]
        ],
        sideEffectsParallelism: Int
      ): Effect[StateWrapper[State]] = {
      if (!state.waitingForProcessingCompletion) {
        val result = Try(logic.processInput(state.innerState, flowInput.in))
        handleInputImpl(state, result, actorContext, flowInput.replyTo, sideEffectsParallelism)
      } else Effect.stash()
    }

    private[spekka] def handleInputAsync[State, In, Out, Command](
        state: StateWrapper[State],
        flowInput: StatefulFlowHandler.ProcessFlowInput[In, Out],
        logic: StatefulFlowLogic.DurableStateAsync[State, In, Out, Command],
        actorContext: ActorContext[
          StatefulFlowHandler.Protocol[In, Out, Command, AkkaPersistenceBackendProtocol]
        ],
        sideEffectsParallelism: Int
      ): Effect[StateWrapper[State]] = {
      if (!state.waitingForProcessingCompletion) {
        val self = actorContext.self

        Try(logic.processInput(state.innerState, flowInput.in)) match {
          case Success(resultF) =>
            resultF.value match {
              case Some(result) =>
                handleInputImpl(
                  state,
                  result,
                  actorContext,
                  flowInput.replyTo,
                  sideEffectsParallelism
                )

              case None =>
                state.waitingForProcessingCompletion = true
                resultF.andThen { case res =>
                  self.tell(
                    DurableState.InputProcessingResultReady(res, flowInput.replyTo)
                  )
                }(actorContext.executionContext)
                Effect.none
            }

          case Failure(ex) =>
            actorContext.log.error("Failure handling input", ex)
            Effect.reply(flowInput.replyTo)(StatusReply.error(ex))
        }
      } else Effect.stash()
    }

    private[spekka] def handleCommandImpl[State, In, Out, Command](
        state: StateWrapper[State],
        result: Try[StatefulFlowLogic.DurableState.ProcessingResult[State, Nothing]],
        actorContext: ActorContext[
          StatefulFlowHandler.Protocol[In, Out, Command, AkkaPersistenceBackendProtocol]
        ],
        sideEffectsParallelism: Int
      ): Effect[StateWrapper[State]] = {
      result match {
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
          )(actorContext.executionContext)

        case Failure(ex) =>
          actorContext.log.error("Failure handling command", ex)
          Effect.none
      }
    }

    private[spekka] def handleCommandSync[State, In, Out, Command](
        state: StateWrapper[State],
        commandRequest: StatefulFlowHandler.ProcessCommand[Command],
        logic: StatefulFlowLogic.DurableState[State, In, Out, Command],
        actorContext: ActorContext[
          StatefulFlowHandler.Protocol[In, Out, Command, AkkaPersistenceBackendProtocol]
        ],
        sideEffectsParallelism: Int
      ): Effect[StateWrapper[State]] = {
      if (!state.waitingForProcessingCompletion) {
        handleCommandImpl(
          state,
          Try(logic.processCommand(state.innerState, commandRequest.command)),
          actorContext,
          sideEffectsParallelism
        )
      } else Effect.stash()
    }

    private[spekka] def handleCommandAsync[State, In, Out, Command](
        state: StateWrapper[State],
        commandRequest: StatefulFlowHandler.ProcessCommand[Command],
        logic: StatefulFlowLogic.DurableStateAsync[State, In, Out, Command],
        actorContext: ActorContext[
          StatefulFlowHandler.Protocol[In, Out, Command, AkkaPersistenceBackendProtocol]
        ],
        sideEffectsParallelism: Int
      ): Effect[StateWrapper[State]] = {
      if (!state.waitingForProcessingCompletion) {
        Try(logic.processCommand(state.innerState, commandRequest.command)) match {
          case Success(resultF) =>
            resultF.value match {
              case Some(result) =>
                handleCommandImpl(state, result, actorContext, sideEffectsParallelism)
              case None =>
                state.waitingForProcessingCompletion = true
                resultF.andThen { case res =>
                  actorContext.self.tell(
                    DurableState.CommandProcessingResultReady(res)
                  )
                }(actorContext.executionContext)
                Effect.none
            }

          case Failure(ex) =>
            actorContext.log.error("Failure handling command", ex)
            Effect.none
        }
      } else Effect.stash()
    }

    private[spekka] def behaviorFactoryProto[State, In, Out, Command](
        entityKind: String,
        entityId: String,
        initialState: () => State,
        storagePlugin: PersistencePlugin,
        sideEffectsParallelism: Int,
        snapshotAdapter: Option[SnapshotAdapter[State]],
        inputHandler: (
            StateWrapper[State],
            StatefulFlowHandler.ProcessFlowInput[In, Out],
            ActorContext[
              StatefulFlowHandler.Protocol[In, Out, Command, AkkaPersistenceBackendProtocol]
            ]
          ) => Effect[StateWrapper[State]],
        inputProcessingReadyHandler: (
            StateWrapper[State],
            InputProcessingResultReady[State, Out],
            ActorContext[
              StatefulFlowHandler.Protocol[In, Out, Command, AkkaPersistenceBackendProtocol]
            ]
          ) => Effect[StateWrapper[State]],
        commandHandler: (
            StateWrapper[State],
            StatefulFlowHandler.ProcessCommand[Command],
            ActorContext[
              StatefulFlowHandler.Protocol[In, Out, Command, AkkaPersistenceBackendProtocol]
            ]
          ) => Effect[StateWrapper[State]],
        commandProcessingReadyHandler: (
            StateWrapper[State],
            CommandProcessingResultReady[State],
            ActorContext[
              StatefulFlowHandler.Protocol[In, Out, Command, AkkaPersistenceBackendProtocol]
            ]
          ) => Effect[StateWrapper[State]]
      ): Behavior[StatefulFlowHandler.Protocol[In, Out, Command, AkkaPersistenceBackendProtocol]] = {
      Behaviors.setup { actorContext =>
        implicit val ec: scala.concurrent.ExecutionContext = actorContext.executionContext

        val baseBehavior = DurableStateBehavior[
          StatefulFlowHandler.Protocol[In, Out, Command, AkkaPersistenceBackendProtocol],
          StateWrapper[State]
        ](
          PersistenceId(entityKind, entityId),
          StateWrapper.recoveredState(initialState()),
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
                handleBeforeSideEffect(
                  state,
                  result,
                  successAction,
                  failureAction,
                  actorContext.self,
                  sideEffectsParallelism
                )

              case AfterSideEffectCompleted(successAction) =>
                handleAfterSideEffect(state, successAction)

              case SideEffectFailure(ex, failureAction) =>
                handleSideEffectFailure(state, failureAction, ex, actorContext.log)

              case flowInput: StatefulFlowHandler.ProcessFlowInput[In, Out] =>
                inputHandler(state, flowInput, actorContext)

              case msg: InputProcessingResultReady[State @unchecked, Out @unchecked] =>
                inputProcessingReadyHandler(state, msg, actorContext)

              case commandRequest: StatefulFlowHandler.ProcessCommand[Command] =>
                commandHandler(state, commandRequest, actorContext)

              case msg: CommandProcessingResultReady[State @unchecked] =>
                commandProcessingReadyHandler(state, msg, actorContext)

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

    private[spekka] def behaviorFactory[State, In, Out, Command](
        entityKind: String,
        entityId: String,
        logic: StatefulFlowLogic.DurableState[State, In, Out, Command],
        storagePlugin: PersistencePlugin,
        sideEffectsParallelism: Int,
        snapshotAdapter: Option[SnapshotAdapter[State]]
      ): Behavior[StatefulFlowHandler.Protocol[In, Out, Command, AkkaPersistenceBackendProtocol]] = {
      behaviorFactoryProto[State, In, Out, Command](
        entityKind,
        entityId,
        () => logic.initialState(entityKind, entityId),
        storagePlugin,
        sideEffectsParallelism,
        snapshotAdapter,
        (state, flowInput, actorContext) =>
          handleInputSync(state, flowInput, logic, actorContext, sideEffectsParallelism),
        (_, _, _) =>
          throw new IllegalStateException(
            "Inconsistency in AkkaPersistenceStatefulFlowBackend.DurableState"
          ),
        (state, commandRequest, actorContext) =>
          handleCommandSync(state, commandRequest, logic, actorContext, sideEffectsParallelism),
        (_, _, _) =>
          throw new IllegalStateException(
            "Inconsistency in AkkaPersistenceStatefulFlowBackend.DurableState"
          )
      )
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
      snapshotAdapter: Option[SnapshotAdapter[State]]
    ) extends StatefulFlowBackend.DurableState[State, DurableState.AkkaPersistenceBackendProtocol] {
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

  /** An implementation of `StatefulFlowBackend.DurableStateAsync` based on Akka Persistence Durable
    * State.
    */
  object DurableStateAsync {
    private[spekka] def behaviorFactory[State, In, Out, Command](
        entityKind: String,
        entityId: String,
        logic: StatefulFlowLogic.DurableStateAsync[State, In, Out, Command],
        storagePlugin: DurableState.PersistencePlugin,
        sideEffectsParallelism: Int,
        snapshotAdapter: Option[SnapshotAdapter[State]],
        initializationTimeout: FiniteDuration
      ): Behavior[StatefulFlowHandler.Protocol[In, Out, Command, DurableState.AkkaPersistenceBackendProtocol]] = {
      StatefulFlowBackend.FutureBehavior(
        initializationTimeout,
        logic.initialState(entityKind, entityId)
      ) { state =>
        DurableState.behaviorFactoryProto[State, In, Out, Command](
          entityKind,
          entityId,
          () => state,
          storagePlugin,
          sideEffectsParallelism,
          snapshotAdapter,
          (state, flowInput, actorContext) =>
            DurableState
              .handleInputAsync(state, flowInput, logic, actorContext, sideEffectsParallelism),
          (state, msg, actorContext) =>
            DurableState
              .handleInputImpl(
                state,
                msg.result,
                actorContext,
                msg.replyTo,
                sideEffectsParallelism
              ),
          (state, commandRequest, actorContext) =>
            DurableState
              .handleCommandAsync(
                state,
                commandRequest,
                logic,
                actorContext,
                sideEffectsParallelism
              ),
          (state, msg, actorContext) =>
            DurableState.handleCommandImpl(state, msg.result, actorContext, sideEffectsParallelism)
        )
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
        sideEffectsParallelism: Int = 1,
        initializationTimeout: FiniteDuration = 30.seconds
      ): DurableStateAsync[State] =
      new DurableStateAsync(
        storagePlugin,
        sideEffectsParallelism,
        None,
        initializationTimeout
      )
  }

  /** An `StatefulFlowBackend.DurableStateAsync` implementation based on Akka Persistence Durable
    * State.
    */
  class DurableStateAsync[State] private[spekka] (
      storagePlugin: DurableState.PersistencePlugin,
      sideEffectsParallelism: Int,
      snapshotAdapter: Option[SnapshotAdapter[State]],
      initializationTimeout: FiniteDuration
    ) extends StatefulFlowBackend.DurableStateAsync[
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
        snapshotAdapter,
        initializationTimeout
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
        snapshotAdapter,
        initializationTimeout
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
        Some(snapshotAdapter),
        initializationTimeout
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
        Some(new SerializedDataSnapshotAdapter(snapshotCodec)),
        initializationTimeout
      )
  }
}
