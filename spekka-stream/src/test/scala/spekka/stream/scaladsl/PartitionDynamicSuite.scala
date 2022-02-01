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

package spekka.stream.scaladsl

import akka.NotUsed
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import spekka.stream.ExtendedContext
import spekka.stream.SeqFlow
import spekka.stream.StreamSuite

import scala.concurrent.duration._

object PartitionDynamicSuite {
  val config = """
    | akka.stream.materializer.initial-input-buffer-size = 2
    | akka.stream.materializer.max-input-buffer-size = 2
    | """.stripMargin
}

class PartitionDynamicSuite
    extends StreamSuite(
      "PartitionDynamic",
      PartitionDynamicSuite.config
    ) {
  import FlowWithExtendedContext.syntax._
  implicit val ec = system.dispatcher

  def flattenContext[T, Ctx](seq: Seq[(T, ExtendedContext[Ctx])]): Seq[(T, Ctx)] = {
    seq.map { case (t, ctx) => t -> ctx.innerContext }
  }

  test("data is correctly partitioned for single key") {
    assertAllStagesStopped {
      val (evenProbe, evenFlow) = SeqFlow[Int, ExtendedContext[Long]]()
      val (oddProbe, oddFlow) = SeqFlow[Int, ExtendedContext[Long]]()

      val partitionedFlow = Partition.dynamic[Int, Int, Long, Boolean, NotUsed](
        (v, _) => v % 2 == 0,
        isEven =>
          if (isEven) evenFlow.asFlowWithExtendedContextUnsafe
          else oddFlow.asFlowWithExtendedContextUnsafe,
        PartitionDynamic.CompletionCriteria.never
      )

      val src = Source(1 to 100).zipWithIndex.map { case (v, ctx) => v -> ExtendedContext(ctx) }
      val doneF = src.via(partitionedFlow).runWith(Sink.seq)

      val res = flattenContext(doneF.futureValue)
      val resEven = flattenContext(evenProbe.expectUpstreamFinished())
      val resOdd = flattenContext(oddProbe.expectUpstreamFinished())

      val expectedRes = (1 to 100).zipWithIndex.map { case (v, ctx) => v -> ctx.toLong }
      val expectedEven = expectedRes.filter(_._1 % 2 == 0)
      val expectedOdd = expectedRes.filter(_._1 % 2 != 0)

      res should contain theSameElementsAs expectedRes
      (resEven should contain).theSameElementsInOrderAs(expectedEven)
      (resOdd should contain).theSameElementsInOrderAs(expectedOdd)
    }
  }

  test("data is correctly partitioned for multi keys with multicast") {
    assertAllStagesStopped {
      val (evenProbe, evenFlow) = SeqFlow[Int, ExtendedContext[Long]]()
      val (oddProbe, oddFlow) = SeqFlow[Int, ExtendedContext[Long]]()

      val partitionedFlow = Partition.dynamicMulti[Int, Int, Long, Boolean, NotUsed](
        (v, _, keys) => if (v == 50) keys else Set(v % 2 == 0),
        isEven =>
          if (isEven) evenFlow.asFlowWithExtendedContextUnsafe
          else oddFlow.asFlowWithExtendedContextUnsafe,
        FlowWithExtendedContext[Int, Long].map(_ =>
          throw new IllegalStateException("Should not happen!")
        ),
        PartitionDynamic.CompletionCriteria.never
      )

      val src = Source(1 to 100).zipWithIndex.map { case (v, ctx) => v -> ExtendedContext(ctx) }
      val doneF = src.via(partitionedFlow).runWith(Sink.seq)

      val res = flattenContext(doneF.futureValue)
      val resEven = flattenContext(evenProbe.expectUpstreamFinished())
      val resOdd = flattenContext(oddProbe.expectUpstreamFinished())

      val expectedValues = (1 to 100).zipWithIndex.map { case (v, ctx) => v -> ctx.toLong }
      val expectedRes = expectedValues.map {
        case (50, ctx) => List(50, 50) -> ctx
        case (v, ctx) => List(v) -> ctx
      }
      val expectedEven = expectedValues.filter { case (v, _) => v % 2 == 0 || v == 50 }
      val expectedOdd = expectedValues.filter { case (v, _) => v % 2 != 0 || v == 50 }

      res should contain theSameElementsAs expectedRes
      (resEven should contain).theSameElementsInOrderAs(expectedEven)
      (resOdd should contain).theSameElementsInOrderAs(expectedOdd)
    }
  }

  test("data is correctly partitioned for multi keys with empty set") {
    assertAllStagesStopped {
      val (evenProbe, evenFlow) = SeqFlow[Int, ExtendedContext[Long]]()
      val (oddProbe, oddFlow) = SeqFlow[Int, ExtendedContext[Long]]()
      val (noneProbe, noneFlow) = SeqFlow[Int, ExtendedContext[Long]]()

      val partitionedFlow = Partition.dynamicMulti[Int, Int, Long, Boolean, NotUsed](
        (v, _, _) => if (v == 50) Set.empty else Set(v % 2 == 0),
        isEven =>
          if (isEven) evenFlow.asFlowWithExtendedContextUnsafe
          else oddFlow.asFlowWithExtendedContextUnsafe,
        noneFlow.map { case (v, ctx) => -v -> ctx }.asFlowWithExtendedContextUnsafe,
        PartitionDynamic.CompletionCriteria.never
      )

      val src = Source(1 to 100).zipWithIndex.map { case (v, ctx) => v -> ExtendedContext(ctx) }
      val doneF = src.via(partitionedFlow).runWith(Sink.seq)

      val res = flattenContext(doneF.futureValue)
      val resEven = flattenContext(evenProbe.expectUpstreamFinished())
      val resOdd = flattenContext(oddProbe.expectUpstreamFinished())
      val resNone = flattenContext(noneProbe.expectUpstreamFinished())

      val expectedValues = (1 to 100).zipWithIndex.map { case (v, ctx) => v -> ctx.toLong }
      val expectedRes = expectedValues.map {
        case (50, ctx) => List(-50) -> ctx
        case (v, ctx) => List(v) -> ctx
      }
      val expectedEven = expectedValues.filter { case (v, _) => v % 2 == 0 && v != 50 }
      val expectedOdd = expectedValues.filter { case (v, _) => v % 2 != 0 && v != 50 }
      val expectedNone = expectedValues.filter { case (v, _) => v == 50 }

      res should contain theSameElementsAs expectedRes
      (resEven should contain).theSameElementsInOrderAs(expectedEven)
      (resOdd should contain).theSameElementsInOrderAs(expectedOdd)
      (resNone should contain).theSameElementsInOrderAs(expectedNone)
    }
  }

  test("unhandled completion in partition flows fails the stream") {
    assertAllStagesStopped {
      val (_, evenFlow) = SeqFlow[Int, ExtendedContext[Long]]()
      val (_, oddFlow) = SeqFlow[Int, ExtendedContext[Long]]()

      val cancellingEvenFlow = Flow[(Int, ExtendedContext[Long])].take(10).via(evenFlow)

      val partitionedFlow = Partition.dynamic[Int, Int, Long, Boolean, NotUsed](
        (v, _) => v % 2 == 0,
        isEven =>
          if (isEven) cancellingEvenFlow.asFlowWithExtendedContextUnsafe
          else oddFlow.asFlowWithExtendedContextUnsafe,
        PartitionDynamic.CompletionCriteria.never
      )

      val src = Source(1 to 100).zipWithIndex.map { case (v, ctx) => v -> ExtendedContext(ctx) }
      val doneF = src.via(partitionedFlow).runWith(Sink.seq)

      doneF.failed.futureValue shouldBe a[IllegalStateException]
    }
  }

  test("complete partition on input message") {
    assertAllStagesStopped {
      val (evenProbe, evenFlow) = SeqFlow[Int, ExtendedContext[Long]]()
      val (oddProbe, oddFlow) = SeqFlow[Int, ExtendedContext[Long]]()

      val partitionedFlow = Partition.dynamic[Int, Int, Long, Boolean, NotUsed](
        (v, _) => v % 2 == 0,
        isEven =>
          if (isEven) evenFlow.asFlowWithExtendedContextUnsafe
          else oddFlow.asFlowWithExtendedContextUnsafe,
        PartitionDynamic.CompletionCriteria.onInput(_ == 10)
      )

      val publisher = TestPublisher.probe[(Int, ExtendedContext[Long])]()
      val doneF = Source.fromPublisher(publisher).via(partitionedFlow).runWith(Sink.seq)
      (1 to 10).zipWithIndex.foreach { case (v, ctx) =>
        publisher.sendNext(v -> ExtendedContext(ctx.toLong))
      }

      val resEven1 = flattenContext(evenProbe.expectUpstreamFinished())

      (11 to 100).zipWithIndex.foreach { case (v, ctx) =>
        publisher.sendNext(v -> ExtendedContext((ctx + 10).toLong))
      }
      publisher.sendComplete()

      val res = flattenContext(doneF.futureValue)
      val resEven2 = flattenContext(evenProbe.expectUpstreamFinished())
      val resOdd = flattenContext(oddProbe.expectUpstreamFinished())

      val expectedRes = (1 to 100).zipWithIndex.map { case (v, ctx) => v -> ctx.toLong }
      val expectedEven1 = expectedRes.filter(_._1 % 2 == 0).filter(_._1 <= 10)
      val expectedEven2 = expectedRes.filter(_._1 % 2 == 0).filter(_._1 > 10)
      val expectedOdd = expectedRes.filter(_._1 % 2 != 0)

      res should contain theSameElementsAs expectedRes
      (resEven1 should contain).theSameElementsInOrderAs(expectedEven1)
      (resEven2 should contain).theSameElementsInOrderAs(expectedEven2)
      (resOdd should contain).theSameElementsInOrderAs(expectedOdd)
    }
  }

  test("complete partition on output message") {
    import FlowWithExtendedContext.syntax._
    assertAllStagesStopped {
      val (evenProbe, evenFlow) = SeqFlow[Int, ExtendedContext[Long]]()
      val (oddProbe, oddFlow) = SeqFlow[Int, ExtendedContext[Long]]()

      val partitionedFlow = Partition.dynamic[Int, String, Long, Boolean, NotUsed](
        (v, _) => v % 2 == 0,
        isEven => {
          val flow =
            if (isEven) evenFlow.asFlowWithExtendedContextUnsafe
            else oddFlow.asFlowWithExtendedContextUnsafe

          flow.toFlowWithContext.map(_.toString).asFlowWithExtendedContextUnsafe
        },
        PartitionDynamic.CompletionCriteria.onOutput(_ == "10")
      )

      val publisher = TestPublisher.probe[(Int, ExtendedContext[Long])]()
      val doneF = Source.fromPublisher(publisher).via(partitionedFlow).runWith(Sink.seq)
      (1 to 10).zipWithIndex.foreach { case (v, ctx) =>
        publisher.sendNext(v -> ExtendedContext(ctx.toLong))
      }

      val resEven1 = flattenContext(evenProbe.expectUpstreamFinished())

      (11 to 100).zipWithIndex.foreach { case (v, ctx) =>
        publisher.sendNext(v -> ExtendedContext((ctx + 10).toLong))
      }
      publisher.sendComplete()

      val res = flattenContext(doneF.futureValue)
      val resEven2 = flattenContext(evenProbe.expectUpstreamFinished())
      val resOdd = flattenContext(oddProbe.expectUpstreamFinished())

      val expectedRes = (1 to 100).zipWithIndex.map { case (v, ctx) => v -> ctx.toLong }
      val expectedResStr = expectedRes.map { case (v, ctx) => v.toString -> ctx }
      val expectedEven1 = expectedRes.filter(_._1 % 2 == 0).filter(_._1 <= 10)
      val expectedEven2 = expectedRes.filter(_._1 % 2 == 0).filter(_._1 > 10)
      val expectedOdd = expectedRes.filter(_._1 % 2 != 0)

      res should contain theSameElementsAs expectedResStr
      (resEven1 should contain).theSameElementsInOrderAs(expectedEven1)
      (resEven2 should contain).theSameElementsInOrderAs(expectedEven2)
      (resOdd should contain).theSameElementsInOrderAs(expectedOdd)
    }
  }

  test("complete partition on idle timeout") {
    assertAllStagesStopped {
      val (evenProbe, evenFlow) = SeqFlow[Int, ExtendedContext[Long]]()
      val (oddProbe, oddFlow) = SeqFlow[Int, ExtendedContext[Long]]()

      val partitionedFlow = Partition.dynamic[Int, Int, Long, Boolean, NotUsed](
        (v, _) => v % 2 == 0,
        isEven =>
          if (isEven) evenFlow.asFlowWithExtendedContextUnsafe
          else oddFlow.asFlowWithExtendedContextUnsafe,
        PartitionDynamic.CompletionCriteria.onIdle(100.millis)
      )

      val publisher = TestPublisher.probe[(Int, ExtendedContext[Long])]()
      val doneF = Source.fromPublisher(publisher).via(partitionedFlow).runWith(Sink.seq)
      (1 to 10).zipWithIndex.foreach { case (v, ctx) =>
        publisher.sendNext(v -> ExtendedContext(ctx.toLong))
      }

      val resEven1 = flattenContext(evenProbe.expectUpstreamFinished())
      val resOdd1 = flattenContext(oddProbe.expectUpstreamFinished())

      (11 to 100).zipWithIndex.foreach { case (v, ctx) =>
        publisher.sendNext(v -> ExtendedContext((ctx + 10).toLong))
      }
      publisher.sendComplete()

      val res = flattenContext(doneF.futureValue)
      val resEven2 = flattenContext(evenProbe.expectUpstreamFinished())
      val resOdd2 = flattenContext(oddProbe.expectUpstreamFinished())

      val expectedRes = (1 to 100).zipWithIndex.map { case (v, ctx) => v -> ctx.toLong }
      val expectedEven1 = expectedRes.filter(_._1 % 2 == 0).filter(_._1 <= 10)
      val expectedEven2 = expectedRes.filter(_._1 % 2 == 0).filter(_._1 > 10)
      val expectedOdd1 = expectedRes.filter(_._1 % 2 != 0).filter(_._1 <= 10)
      val expectedOdd2 = expectedRes.filter(_._1 % 2 != 0).filter(_._1 > 10)

      res should contain theSameElementsAs expectedRes
      (resEven1 should contain).theSameElementsInOrderAs(expectedEven1)
      (resEven2 should contain).theSameElementsInOrderAs(expectedEven2)
      (resOdd1 should contain).theSameElementsInOrderAs(expectedOdd1)
      (resOdd2 should contain).theSameElementsInOrderAs(expectedOdd2)
    }
  }
}
