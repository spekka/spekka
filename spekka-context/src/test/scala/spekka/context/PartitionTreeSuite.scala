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

import akka.Done
import akka.NotUsed
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.TestSubscriber
import spekka.test.SpekkaSuite

class PartitionTreeBuilderSuite extends SpekkaSuite("PartitionTreeBuilderSuite") {

  import scala.concurrent.ExecutionContext.Implicits.global
  import PartitionTree._

  case class Input(k1: Int, k2: String, k3: Boolean, value: Int) {
    def toOutput[KS](ks: KS): Output[KS] = Output(ks, this)
  }
  case class Output[KS](keys: KS, input: Input)

  test("single layer dynamic auto") {
    val flow = Partition
      .treeBuilder[Input, Long]
      .dynamicAuto { case (in, _) => in.k1 }
      .build { case ks => FlowWithExtendedContext[Input, Long].map(_.toOutput(ks)) }

    val inputs = List(
      Input(1, "a", true, 1),
      Input(2, "b", false, 2),
      Input(1, "c", false, 3),
      Input(2, "d", true, 4)
    )

    val expected = inputs.zipWithIndex.map { case (i, ctx) =>
      Output(i.k1 :: KNil, i) -> ctx.toLong
    }

    val (_, result) =
      Source(inputs).zipWithIndex.viaMat(flow)(Keep.right).toMat(Sink.seq)(Keep.both).run()
    result.futureValue should contain theSameElementsAs expected
  }

  test("single layer dynamic manual pre instantiated") {
    val flow = Partition
      .treeBuilder[Input, Long]
      .dynamicManual({ case (in, _) => in.k1 }, Set(1, 2))
      .build { case ks => FlowWithExtendedContext[Input, Long].map(_.toOutput(ks)) }

    val inputs = List(
      Input(1, "a", true, 1),
      Input(2, "b", false, 2),
      Input(1, "c", false, 3),
      Input(2, "d", true, 4)
    )

    val expected = inputs.zipWithIndex.map { case (i, ctx) =>
      Some(Output(i.k1 :: KNil, i)) -> ctx.toLong
    }

    val (_, result) =
      Source(inputs).zipWithIndex.viaMat(flow)(Keep.right).toMat(Sink.seq)(Keep.both).run()
    result.futureValue should contain theSameElementsAs expected
  }

  test("single layer dynamic manual partially instantiated") {
    val flow = Partition
      .treeBuilder[Input, Long]
      .dynamicManual({ case (in, _) => in.k1 }, Set(1))
      .build { case ks => FlowWithExtendedContext[Input, Long].map(_.toOutput(ks)) }

    val inputs = List(
      Input(1, "a", true, 1),
      Input(2, "b", false, 2),
      Input(1, "c", false, 3),
      Input(2, "d", true, 4)
    )

    val expected = inputs.zipWithIndex.map {
      case (i, ctx) if i.k1 == 1 =>
        Some(Output(i.k1 :: KNil, i)) -> ctx.toLong
      case (_, ctx) => None -> ctx.toLong
    }

    val (_, result) =
      Source(inputs).zipWithIndex.viaMat(flow)(Keep.right).toMat(Sink.seq)(Keep.both).run()
    result.futureValue should contain theSameElementsAs expected
  }

  test("single layer dynamic manual dynamically instantiated") {
    val flow = Partition
      .treeBuilder[Input, Long]
      .dynamicManual({ case (in, _) => in.k1 }, Set.empty)
      .build { case ks => FlowWithExtendedContext[Input, Long].map(_.toOutput(ks)) }

    val sourceProbe = TestPublisher.probe[(Input, Long)]()
    val sinkProbe = TestSubscriber.probe[(Option[Output[Int :: KNil]], Long)]()

    val control =
      Source
        .fromPublisher(sourceProbe)
        .viaMat(flow)(Keep.right)
        .to(Sink.fromSubscriber(sinkProbe))
        .run()

    sinkProbe.request(2)
    sourceProbe.sendNext(Input(1, "a", true, 1) -> 1)
    sinkProbe.expectNext() shouldBe None -> 1
    sourceProbe.sendNext(Input(2, "b", true, 2) -> 2)
    sinkProbe.expectNext() shouldBe None -> 2

    (for {
      r1 <- control.materializeKey(1)
      r2 <- control.materializeKey(2)
    } yield r1 -> r2).run.futureValue shouldBe Some(NotUsed -> NotUsed)

    sinkProbe.request(2)
    sourceProbe.sendNext(Input(1, "a", true, 3) -> 3)
    sinkProbe.expectNext() shouldBe Some(Output(1 :: KNil, Input(1, "a", true, 3))) -> 3
    sourceProbe.sendNext(Input(2, "b", true, 4) -> 4)
    sinkProbe.expectNext() shouldBe Some(Output(2 :: KNil, Input(2, "b", true, 4))) -> 4

    (for {
      r1 <- control.completeKey(1)
    } yield r1).run.futureValue shouldBe Some(Done)

    sinkProbe.request(2)
    sourceProbe.sendNext(Input(1, "a", true, 5) -> 5)
    sinkProbe.expectNext() shouldBe None -> 5
    sourceProbe.sendNext(Input(2, "b", true, 6) -> 6)
    sinkProbe.expectNext() shouldBe Some(Output(2 :: KNil, Input(2, "b", true, 6))) -> 6

    sinkProbe.request(1)
    sourceProbe.sendComplete()
    sinkProbe.expectComplete()
  }

  test("dual layer dynamic manual dynamically instantiated") {
    val flow = Partition
      .treeBuilder[Input, Long]
      .dynamicManual({ case (in, _) => in.k1 }, Set.empty)
      .dynamicManual({ case (in, _) => in.k2 }, Set.empty)
      .build { case ks => FlowWithExtendedContext[Input, Long].map(_.toOutput(ks)) }

    val sourceProbe = TestPublisher.probe[(Input, Long)]()
    val sinkProbe = TestSubscriber.probe[(Option[Output[String :: Int :: KNil]], Long)]()

    val control =
      Source
        .fromPublisher(sourceProbe)
        .viaMat(flow)(Keep.right)
        .to(Sink.fromSubscriber(sinkProbe))
        .run()

    sinkProbe.request(2)
    sourceProbe.sendNext(Input(1, "a", true, 1) -> 1)
    sinkProbe.expectNext() shouldBe None -> 1
    sourceProbe.sendNext(Input(2, "b", true, 2) -> 2)
    sinkProbe.expectNext() shouldBe None -> 2

    (for {
      c1 <- control.atKeyForced(1)
      ra <- c1.materializeKey("a")
      c2 <- control.atKeyForced(2)
      rb <- c2.materializeKey("b")
    } yield ra -> rb).run.futureValue shouldBe Some(NotUsed -> NotUsed)

    sinkProbe.request(4)
    sourceProbe.sendNext(Input(1, "a", true, 3) -> 3)
    sinkProbe.expectNext() shouldBe Some(Output("a" :: 1 :: KNil, Input(1, "a", true, 3))) -> 3
    sourceProbe.sendNext(Input(1, "b", true, 4) -> 4)
    sinkProbe.expectNext() shouldBe None -> 4
    sourceProbe.sendNext(Input(2, "a", true, 5) -> 5)
    sinkProbe.expectNext() shouldBe None -> 5
    sourceProbe.sendNext(Input(2, "b", true, 6) -> 6)
    sinkProbe.expectNext() shouldBe Some(Output("b" :: 2 :: KNil, Input(2, "b", true, 6))) -> 6

    (for {
      c1 <- control.atKey(1)
      r <- c1.completeKey("a")
    } yield r).run.futureValue shouldBe Some(Done)

    sinkProbe.request(2)
    sourceProbe.sendNext(Input(1, "a", true, 7) -> 7)
    sinkProbe.expectNext() shouldBe None -> 7
    sourceProbe.sendNext(Input(2, "b", true, 8) -> 8)
    sinkProbe.expectNext() shouldBe Some(Output("b" :: 2 :: KNil, Input(2, "b", true, 8))) -> 8

    sinkProbe.request(1)
    sourceProbe.sendComplete()
    sinkProbe.expectComplete()
  }

  test("dual layer multi dynamic auto") {
    val flow = Partition
      .treeBuilder[Input, Long]
      .dynamicAutoMulticast[Int] { case (in, _, ks) =>
        if (in.k1 == 0) ks
        else Set(in.k1)
      }
      .dynamicAutoMulticast[String] { case (in, _, ks) =>
        if (in.k2 == "*") ks
        else Set(in.k2)
      }
      .build { case ks => FlowWithExtendedContext[Input, Long].map(_ => ks) }

    val inputs = List(
      Input(1, "a", true, 1),
      Input(1, "b", true, 2),
      Input(1, "*", false, 3),
      Input(2, "a", true, 4),
      Input(2, "b", true, 5),
      Input(2, "*", false, 6),
      Input(0, "*", false, 7)
    )

    val expected = List(
      List("a" :: 1 :: KNil),
      List("b" :: 1 :: KNil),
      List("a" :: 1 :: KNil, "b" :: 1 :: KNil),
      List("a" :: 2 :: KNil),
      List("b" :: 2 :: KNil),
      List("a" :: 2 :: KNil, "b" :: 2 :: KNil),
      List("a" :: 1 :: KNil, "b" :: 1 :: KNil, "a" :: 2 :: KNil, "b" :: 2 :: KNil)
    ).zipWithIndex.map { case (ks, ctx) => ks -> ctx.toLong }

    val (_, result) =
      Source(inputs).zipWithIndex.viaMat(flow)(Keep.right).toMat(Sink.seq)(Keep.both).run()
    val data = result.futureValue

    val dataSorted = data.map { case (ks, ctx) =>
      ks.toList.sortBy { case k2 :: k1 :: KNil => k1 -> k2 } -> ctx
    }
    dataSorted should contain theSameElementsAs expected
  }
}
