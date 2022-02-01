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
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.StreamTestKit.assertAllStagesStopped
import spekka.stream.SeqFlow
import spekka.stream.StreamSuite
import spekka.stream.ExtendedContext

object PartitionStaticSuite {
  val config = """
    | akka.stream.materializer.initial-input-buffer-size = 2
    | akka.stream.materializer.max-input-buffer-size = 2
    | """.stripMargin
}

class PartitionStaticSuite
    extends StreamSuite(
      "PartitionStatic",
      PartitionStaticSuite.config
    ) {
  import FlowWithExtendedContext.syntax._
  implicit val ec = system.dispatcher

  def flattenContext[T, Ctx](seq: Seq[(T, ExtendedContext[Ctx])]): Seq[(T, Ctx)] = {
    seq.map { case (t, ctx) => t -> ctx.innerContext }
  }

  test("data is correctly partitioned for single key") {
    assertAllStagesStopped {
      val (p1Probe, p1Flow) = SeqFlow[Int, ExtendedContext[Long]]()
      val (p2Probe, p2Flow) = SeqFlow[Int, ExtendedContext[Long]]()
      val (puProbe, puFlow) = SeqFlow[Int, ExtendedContext[Long]]()

      val partitionedFlow = Partition.staticMat[Int, Int, Long, Int, NotUsed, NotUsed, NotUsed](
        (v, _) => v % 10,
        puFlow.asFlowWithExtendedContextUnsafe
      )(
        1 -> p1Flow.asFlowWithExtendedContextUnsafe,
        2 -> p2Flow.asFlowWithExtendedContextUnsafe
      )

      val src = Source(1 to 100).zipWithIndex.map { case (v, ctx) => v -> ExtendedContext(ctx) }
      val doneF = src.via(partitionedFlow).runWith(Sink.seq)

      val res = flattenContext(doneF.futureValue)
      val resP1 = flattenContext(p1Probe.expectUpstreamFinished())
      val resP2 = flattenContext(p2Probe.expectUpstreamFinished())
      val resPu = flattenContext(puProbe.expectUpstreamFinished())

      val expectedRes = (1 to 100).zipWithIndex.map { case (v, ctx) => v -> ctx.toLong }
      val expectedP1 = expectedRes.filter(_._1 % 10 == 1)
      val expectedP2 = expectedRes.filter(_._1 % 10 == 2)
      val expectedPu = expectedRes.filter(v => !Set(1, 2).contains(v._1 % 10))

      res should contain theSameElementsAs expectedRes
      (resP1 should contain).theSameElementsInOrderAs(expectedP1)
      (resP2 should contain).theSameElementsInOrderAs(expectedP2)
      (resPu should contain).theSameElementsInOrderAs(expectedPu)
    }
  }

  test("data is correctly partitioned for multi key") {
    assertAllStagesStopped {
      val (p0Probe, p0Flow) = SeqFlow[Int, ExtendedContext[Long]]()
      val (p1Probe, p1Flow) = SeqFlow[Int, ExtendedContext[Long]]()
      val (puProbe, puFlow) = SeqFlow[Int, ExtendedContext[Long]]()

      val partitionedFlow = Partition.staticMultiMat[Int, Int, Long, Int, NotUsed, NotUsed, NotUsed](
        (v, _, _) => Set(v % 10, 0),
        puFlow.asFlowWithExtendedContextUnsafe
      )(
        0 -> p0Flow.asFlowWithExtendedContextUnsafe.map(-_),
        1 -> p1Flow.asFlowWithExtendedContextUnsafe
      )

      val src = Source(1 to 100).zipWithIndex.map { case (v, ctx) => v -> ExtendedContext(ctx) }
      val doneF = src.via(partitionedFlow).runWith(Sink.seq)

      val res = flattenContext(doneF.futureValue)
      val resP0 = flattenContext(p0Probe.expectUpstreamFinished())
      val resP1 = flattenContext(p1Probe.expectUpstreamFinished())
      val resPu = flattenContext(puProbe.expectUpstreamFinished())

      val expectedRes = (1 to 100).zipWithIndex.map { case (v, ctx) => v -> ctx.toLong }
      val expectedP0 = expectedRes
      val expectedP1 = expectedRes.filter(_._1 % 10 == 1)
      val expectedPu = expectedRes.filter(v => !Set(0, 1).contains(v._1 % 10))

      res should contain theSameElementsAs expectedRes.map { case (v, ctx) =>
        (if (v % 10 == 0) List(-v) else List(-v, v)) -> ctx
      }
      (resP0 should contain).theSameElementsInOrderAs(expectedP0)
      (resP1 should contain).theSameElementsInOrderAs(expectedP1)
      (resPu should contain).theSameElementsInOrderAs(expectedPu)
    }
  }


  test("materialized values are correctly combined for single") {
      assertAllStagesStopped {
        val (_, p1Flow) = SeqFlow[Int, ExtendedContext[Long]]()
        val (_, p2Flow) = SeqFlow[Int, ExtendedContext[Long]]()
        val (_, puFlow) = SeqFlow[Int, ExtendedContext[Long]]()

        val partitionedFlow = Partition.staticMat[Int, Int, Long, Int, Int, Int, Int](
          (v, _) => v % 10,
          puFlow.asFlowWithExtendedContextUnsafe.mapMaterializedValue(_ => -1)
        )(
          1 -> p1Flow.asFlowWithExtendedContextUnsafe.mapMaterializedValue(_ => 1),
          2 -> p2Flow.asFlowWithExtendedContextUnsafe.mapMaterializedValue((_ => 2))
        )

        val src = Source(1 to 100).zipWithIndex.map { case (v, ctx) => v -> ExtendedContext(ctx) }
        val ((p1M, p2M, puM), doneF) = src.viaMat(partitionedFlow)(Keep.right).toMat(Sink.seq)(Keep.both).run()

        flattenContext(doneF.futureValue)
        p1M shouldEqual 1
        p2M shouldEqual 2
        puM shouldEqual -1
      }
    }

  test("materialized values are correctly combined for multi") {
      assertAllStagesStopped {
        val (_, p1Flow) = SeqFlow[Int, ExtendedContext[Long]]()
        val (_, p2Flow) = SeqFlow[Int, ExtendedContext[Long]]()
        val (_, puFlow) = SeqFlow[Int, ExtendedContext[Long]]()

        val partitionedFlow = Partition.staticMultiMat[Int, Int, Long, Int, Int, Int, Int](
          (_, _, ks) => ks,
          puFlow.asFlowWithExtendedContextUnsafe.mapMaterializedValue(_ => -1)
        )(
          1 -> p1Flow.asFlowWithExtendedContextUnsafe.mapMaterializedValue(_ => 1),
          2 -> p2Flow.asFlowWithExtendedContextUnsafe.mapMaterializedValue((_ => 2))
        )

        val src = Source(1 to 100).zipWithIndex.map { case (v, ctx) => v -> ExtendedContext(ctx) }
        val ((p1M, p2M, puM), doneF) = src.viaMat(partitionedFlow)(Keep.right).toMat(Sink.seq)(Keep.both).run()

        flattenContext(doneF.futureValue)
        p1M shouldEqual 1
        p2M shouldEqual 2
        puM shouldEqual -1
      }
    }
}
