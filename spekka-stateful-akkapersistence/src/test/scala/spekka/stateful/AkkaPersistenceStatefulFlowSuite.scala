package spekka.stateful

import spekka.test.SpekkaSuite
import akka.persistence.testkit.PersistenceTestKitPlugin
import akka.persistence.testkit.PersistenceTestKitSnapshotPlugin
import com.typesafe.config.ConfigFactory
import akka.actor.typed.ActorSystem

import scala.concurrent.duration._
import akka.stream.scaladsl.Source
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.persistence.typed.scaladsl.RetentionCriteria
import spekka.codec.Encoder
import spekka.codec.Decoder
import scala.util.Success
import scala.util.Failure
import akka.persistence.testkit.scaladsl.PersistenceTestKit
import org.scalatest.BeforeAndAfterEach
import akka.persistence.testkit.scaladsl.SnapshotTestKit
import akka.persistence.typed.PersistenceId
import akka.persistence.testkit.PersistenceTestKitDurableStateStorePlugin
import akka.testkit.TestProbe
import scala.concurrent.Future

object AkkaPersistenceStatefulFlowSuite {
  val config = ConfigFactory
    .parseString("""
    """.stripMargin)
    .withFallback(PersistenceTestKitPlugin.config)
    .withFallback(PersistenceTestKitSnapshotPlugin.config)
    .withFallback(PersistenceTestKitDurableStateStorePlugin.config)
}

class AkkaPersistenceStatefulFlowSuite
    extends SpekkaSuite("AkkaPersistenceStatefulFlow", AkkaPersistenceStatefulFlowSuite.config)
    with BeforeAndAfterEach {

  implicit val typedSystem = ActorSystem.wrap(system)
  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val testEventEncoder = Encoder[TestEvent] { case IncreaseCounterWithTimestamp(ts) =>
    s"increase:${ts}".getBytes()
  }
  implicit val testEventDecoder = Decoder[TestEvent] { bytes =>
    val str = new String(bytes)
    str.split(":") match {
      case Array("increase", ts) => Success(IncreaseCounterWithTimestamp(ts.toLong))
      case _ => Failure(new IllegalArgumentException(s"Unable to decode event: $str"))
    }
  }

  implicit val testStateEncoder = Encoder[TestState] { case TestState(lastTimestamp, counter) =>
    s"${lastTimestamp}:${counter}".getBytes()
  }

  implicit val testStateDecoder = Decoder[TestState] { bytes =>
    val str = new String(bytes)
    str.split(":") match {
      case Array(lastTimestamp, counter) => Success(TestState(lastTimestamp.toLong, counter.toLong))
      case _ => Failure(new IllegalArgumentException(s"Unable to decode state: $str"))
    }
  }

  val inputs = 1.to(10).map(i => TestInput(i.toLong, 1)) :+ TestInput(0L, 1)
  val registry = StatefulFlowRegistry(30.seconds)

  val eventsTestKit = PersistenceTestKit(system)
  val snapshotTestKit = SnapshotTestKit(system)

  override protected def beforeEach(): Unit = {
    eventsTestKit.clearAll()
    snapshotTestKit.clearAll()
  }

  test("event based - simple flow no side effects") {
    val flowProps = EventBasedTestLogic(TestState(0, 0))
      .propsForBackend(
        AkkaPersistenceStatefulFlowBackend
          .EventBased[TestState, TestEvent](
            AkkaPersistenceStatefulFlowBackend.EventBased.PersistencePlugin.CustomStoragePlugin(
              PersistenceTestKitPlugin.PluginId,
              PersistenceTestKitSnapshotPlugin.PluginId
            ),
            RetentionCriteria.snapshotEvery(4, 3).withDeleteEventsOnSnapshot
          )
          .withEventCodec
          .withSnapshotCodec
      )

    val builder = registry.registerStatefulFlowSync("testKind-event", flowProps)

    val (controlF, resF) = Source(inputs)
      .viaMat(builder.flow("1"))(Keep.right)
      .toMat(Sink.seq)(Keep.both)
      .run()

    val events = resF.futureValue.flatten
    val control = controlF.futureValue

    val counter = control.commandWithResult(GetCounter(_)).futureValue

    control.terminate().futureValue

    counter shouldBe 10L
    events shouldBe 1.to(10).map(i => IncreaseCounterWithTimestamp(i.toLong))

    eventsTestKit
      .persistedInStorage(PersistenceId("testKind-event", "1").id)
      .map {
        case data: AkkaPersistenceStatefulFlowBackend.SerializedData =>
          testEventDecoder.decode(data.bytes)
        case _ => throw new IllegalArgumentException("Unexpected serialization")
      }
      .map(_.get) shouldBe events

    snapshotTestKit
      .persistedInStorage(PersistenceId("testKind-event", "1").id)
      .map {
        _._2 match {
          case data: AkkaPersistenceStatefulFlowBackend.SerializedData =>
            testStateDecoder.decode(data.bytes)
          case _ => throw new IllegalArgumentException("Unexpected serialization")
        }
      }
      .map(_.get) should contain theSameElementsAs Set(
      TestState(4, 4),
      TestState(8, 8)
    )

    // In order to test the recovery, lets just restart the flow and ask for the state
    builder.control("1").futureValue shouldBe None
    val (control1F, res1F) = Source
      .single(TestInput(0L, 1))
      .viaMat(builder.flow("1"))(Keep.right)
      .toMat(Sink.seq)(Keep.both)
      .run()

    val events1 = res1F.futureValue.flatten
    val control1 = control1F.futureValue

    val counter1 = control1.commandWithResult(GetCounter(_)).futureValue

    control1.terminate().futureValue

    counter1 shouldBe 10L
    events1 shouldBe empty
  }

  test("event based - simple flow with side effects") {
    val inputSideEffectsProbe = TestProbe()
    val commandSideEffectsProbe = TestProbe()

    val flowProps = EventBasedTestLogic(
      TestState(0, 0),
      inputBeforeSideEffectsF = (state, input) => {
        if (input.timestamp == 5L)
          List(() =>
            Future.successful(inputSideEffectsProbe.ref ! ("before" -> state.lastTimestamp))
          )
        else Nil
      },
      inputAfterSideEffectsF = (state, input) => {
        if (input.timestamp == 5L)
          List(() =>
            Future.successful(inputSideEffectsProbe.ref ! ("after" -> state.lastTimestamp))
          )
        else Nil
      },
      commandBeforeSideEffectsF = (_, _) => {
        List(() => Future.successful(commandSideEffectsProbe.ref ! "before"))
      },
      commandAfterSideEffectsF = (_, _) => {
        List(() => Future.successful(commandSideEffectsProbe.ref ! "after"))
      }
    )
      .propsForBackend(
        AkkaPersistenceStatefulFlowBackend
          .EventBased[TestState, TestEvent](
            AkkaPersistenceStatefulFlowBackend.EventBased.PersistencePlugin.CustomStoragePlugin(
              PersistenceTestKitPlugin.PluginId,
              PersistenceTestKitSnapshotPlugin.PluginId
            ),
            RetentionCriteria.snapshotEvery(4, 3).withDeleteEventsOnSnapshot
          )
          .withEventCodec
          .withSnapshotCodec
      )

    val builder = registry.registerStatefulFlowSync("testKind-event-effects", flowProps)

    val (controlF, resF) = Source(inputs)
      .viaMat(builder.flow("1"))(Keep.right)
      .toMat(Sink.seq)(Keep.both)
      .run()

    inputSideEffectsProbe.expectMsg(("before" -> 4L))
    inputSideEffectsProbe.expectMsg(("after" -> 4L))

    val events = resF.futureValue.flatten
    val control = controlF.futureValue

    commandSideEffectsProbe.expectNoMessage()
    val counter = control.commandWithResult(GetCounter(_)).futureValue
    commandSideEffectsProbe.expectMsg("before")
    commandSideEffectsProbe.expectMsg("after")

    control.terminate().futureValue

    counter shouldBe 10L
    events shouldBe 1.to(10).map(i => IncreaseCounterWithTimestamp(i.toLong))

    eventsTestKit
      .persistedInStorage(PersistenceId("testKind-event-effects", "1").id)
      .map {
        case data: AkkaPersistenceStatefulFlowBackend.SerializedData =>
          testEventDecoder.decode(data.bytes)
        case _ => throw new IllegalArgumentException("Unexpected serialization")
      }
      .map(_.get) shouldBe events

    snapshotTestKit
      .persistedInStorage(PersistenceId("testKind-event-effects", "1").id)
      .map {
        _._2 match {
          case data: AkkaPersistenceStatefulFlowBackend.SerializedData =>
            testStateDecoder.decode(data.bytes)
          case _ => throw new IllegalArgumentException("Unexpected serialization")
        }
      }
      .map(_.get) should contain theSameElementsAs Set(
      TestState(4, 4),
      TestState(8, 8)
    )

    // In order to test the recovery, lets just restart the flow and ask for the state
    builder.control("1").futureValue shouldBe None
    val (control1F, res1F) = Source
      .single(TestInput(0L, 1))
      .viaMat(builder.flow("1"))(Keep.right)
      .toMat(Sink.seq)(Keep.both)
      .run()

    val events1 = res1F.futureValue.flatten
    val control1 = control1F.futureValue

    val counter1 = control1.commandWithResult(GetCounter(_)).futureValue

    control1.terminate().futureValue

    counter1 shouldBe 10L
    events1 shouldBe empty
  }

  test("durable state - simple flow no side effects") {
    val flowProps = DurableStateTestLogic(TestState(0, 0))
      .propsForBackend(
        AkkaPersistenceStatefulFlowBackend
          .DurableState[TestState](
            AkkaPersistenceStatefulFlowBackend.DurableState.PersistencePlugin.CustomStoragePlugin(
              PersistenceTestKitDurableStateStorePlugin.PluginId
            )
          )
          .withSnapshotCodec
      )

    val resultF = for {
      builder <- registry.registerStatefulFlow("testKind-durable", flowProps)

      (controlF, resF) = Source(inputs)
        .viaMat(builder.flow("1"))(Keep.right)
        .toMat(Sink.seq)(Keep.both)
        .run()

      res <- resF
      control <- controlF

      counter <- control.commandWithResult(GetCounter(_))
      _ <- control.terminate()

      _ = counter shouldBe 10L
      _ = res.flatten shouldBe 1.to(10).map(i => IncreaseCounterWithTimestamp(i.toLong))

      // Since there is no testkit for durable state, we need to check that the stet is correctly persisted
      // by running a second stream and checking that at the end the state and the produced outputs are
      // consistent with the state persisted in the first run

      // First lets verify that the persistent actor has been completely deregistered and thus
      // will be recreated by reading the state store on the next run
      controlMissing <- builder.control("1")
      _ = controlMissing shouldBe None

      inputs1 = 1.to(20).map(i => TestInput(i.toLong, 1))

      (control1F, res1F) = Source(inputs1)
        .viaMat(builder.flow("1"))(Keep.right)
        .toMat(Sink.seq)(Keep.both)
        .run()

      res1 <- res1F
      control1 <- control1F

      counter1 <- control1.commandWithResult(GetCounter(_))
      _ <- control1.terminate()

      _ = counter1 shouldBe 20L
      _ = res1.flatten shouldBe 11.to(20).map(i => IncreaseCounterWithTimestamp(i.toLong))
    } yield ()

    resultF.futureValue
  }

  test("durable state - simple flow with side effects") {
    val inputSideEffectsProbe = TestProbe()
    val commandSideEffectsProbe = TestProbe()

    val flowProps = DurableStateTestLogic(
      TestState(0, 0),
      inputBeforeSideEffectsF = (state, input) => {
        if (input.timestamp == 5L)
          List(() =>
            Future.successful(inputSideEffectsProbe.ref ! ("before" -> state.lastTimestamp))
          )
        else Nil
      },
      inputAfterSideEffectsF = (state, input) => {
        if (input.timestamp == 5L)
          List(() =>
            Future.successful(inputSideEffectsProbe.ref ! ("after" -> state.lastTimestamp))
          )
        else Nil
      },
      commandBeforeSideEffectsF = (_, _) => {
        List(() => Future.successful(commandSideEffectsProbe.ref ! "before"))
      },
      commandAfterSideEffectsF = (_, _) => {
        List(() => Future.successful(commandSideEffectsProbe.ref ! "after"))
      }
    )
      .propsForBackend(
        AkkaPersistenceStatefulFlowBackend
          .DurableState[TestState](
            AkkaPersistenceStatefulFlowBackend.DurableState.PersistencePlugin.CustomStoragePlugin(
              PersistenceTestKitDurableStateStorePlugin.PluginId
            )
          )
          .withSnapshotCodec
      )

    val resultF = for {
      builder <- registry.registerStatefulFlow("testKind-durable-effects", flowProps)

      (controlF, resF) = Source(inputs)
        .viaMat(builder.flow("1"))(Keep.right)
        .toMat(Sink.seq)(Keep.both)
        .run()

      _ = inputSideEffectsProbe.expectMsg(("before" -> 4L))
      _ = inputSideEffectsProbe.expectMsg(("after" -> 4L))

      res <- resF
      control <- controlF

      _ = commandSideEffectsProbe.expectNoMessage()
      counter <- control.commandWithResult(GetCounter(_))
      _ = commandSideEffectsProbe.expectMsg("before")
      _ = commandSideEffectsProbe.expectMsg("after")

      _ <- control.terminate()

      _ = counter shouldBe 10L
      _ = res.flatten shouldBe 1.to(10).map(i => IncreaseCounterWithTimestamp(i.toLong))

      // Since there is no testkit for durable state, we need to check that the stet is correctly persisted
      // by running a second stream and checking that at the end the state and the produced outputs are
      // consistent with the state persisted in the first run

      // First lets verify that the persistent actor has been completely deregistered and thus
      // will be recreated by reading the state store on the next run
      controlMissing <- builder.control("1")
      _ = controlMissing shouldBe None

      inputs1 = 1.to(20).map(i => TestInput(i.toLong, 1))

      (control1F, res1F) = Source(inputs1)
        .viaMat(builder.flow("1"))(Keep.right)
        .toMat(Sink.seq)(Keep.both)
        .run()

      res1 <- res1F
      control1 <- control1F

      counter1 <- control1.commandWithResult(GetCounter(_))
      _ <- control1.terminate()

      _ = counter1 shouldBe 20L
      _ = res1.flatten shouldBe 11.to(20).map(i => IncreaseCounterWithTimestamp(i.toLong))
    } yield ()

    resultF.futureValue

  }
}
