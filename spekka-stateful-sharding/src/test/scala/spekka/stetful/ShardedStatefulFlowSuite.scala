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
import akka.actor.typed.ActorSystem
import akka.cluster.sharding.typed.scaladsl.ClusterSharding
import akka.cluster.typed.Cluster
import akka.cluster.typed.Join
import akka.pattern.StatusReply
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import org.scalatest.BeforeAndAfterAll
import spekka.test.SpekkaSuite

import scala.concurrent.duration._

object ShardedStatefulFlowSuite {
  val config = """
    | akka.stream.materializer.initial-input-buffer-size = 2
    | akka.stream.materializer.max-input-buffer-size = 2
    |
    | akka.actor.provider = "cluster"
    | """.stripMargin
}

class ShardedStatefulFlowSuite
    extends SpekkaSuite("ShardedStatefulFlow", ShardedStatefulFlowSuite.config)
    with BeforeAndAfterAll {
  implicit val typedSystem = ActorSystem.wrap(system)
  import scala.concurrent.ExecutionContext.Implicits.global

  case class TestState(lastTimestamp: Long, counter: Long)

  sealed trait TestCommand
  case class GetCounter(replyTo: ActorRef[StatusReply[Long]]) extends TestCommand

  case class TestInput(timestamp: Long, discriminator: Int)

  sealed trait TestEvent
  case class IncreaseCounterWithTimestamp(timestamp: Long) extends TestEvent

  object EventBasedLogic {
    import StatefulFlowLogic.EventBased.ProcessingResult
    def processInput(
        state: TestState,
        in: TestInput
      ): StatefulFlowLogic.EventBased.ProcessingResult[TestEvent] = {
      if (in.timestamp > state.lastTimestamp)
        ProcessingResult.withEvent(
          IncreaseCounterWithTimestamp(in.timestamp)
        )
      else
        ProcessingResult.empty
    }

    def updateState(state: TestState, event: TestEvent): TestState = {
      event match {
        case IncreaseCounterWithTimestamp(timestamp) =>
          state.copy(lastTimestamp = timestamp, counter = state.counter + 1)
      }
    }

    def processCommand(state: TestState, command: TestCommand): ProcessingResult[TestEvent] = {
      command match {
        case GetCounter(replyTo) =>
          replyTo ! StatusReply.success(state.counter)
          ProcessingResult.empty
      }
    }
  }

  override protected def beforeAll(): Unit = {
    val cluster = Cluster.get(typedSystem);
    cluster.manager.tell(Join(cluster.selfMember.address))
  }

  val inputs = 1.to(10).map(i => TestInput(i.toLong, 1)) :+ TestInput(0L, 1)

  // #registry
  val registry = StatefulFlowRegistry(30.seconds)(typedSystem)
  val shardedRegistry =
    ShardedStatefulFlowRegistry(registry, ClusterSharding(typedSystem), 30.seconds)(typedSystem)
  // #registry
  test("simple flow works with sharding") {
    val flowProps = StatefulFlowLogic
      .EventBased(
        () => TestState(0L, 0L),
        (state: TestState, in: TestInput) => EventBasedLogic.processInput(state, in),
        (state: TestState, event: TestEvent) => EventBasedLogic.updateState(state, event),
        (state: TestState, command: TestCommand) => EventBasedLogic.processCommand(state, command)
      )
      .propsForBackend(InMemoryStatefulFlowBackend.EventBased())

    val resultF = for {
      builder <- shardedRegistry.registerStatefulFlow("testKind-event", flowProps)

      (controlF, resF) = Source(inputs)
        .viaMat(builder.flow("1"))(Keep.right)
        .toMat(Sink.seq)(Keep.both)
        .run()

      res <- resF

      control <- controlF

      counter <- control.commandWithResult(GetCounter(_))
    } yield (counter, res.flatten)

    val (counter, events) = resultF.futureValue
    counter shouldBe 10L
    events shouldBe 1.to(10).map(i => IncreaseCounterWithTimestamp(i.toLong))
  }
}
