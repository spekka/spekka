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

package spekka.benchmark

import akka.actor.ActorSystem
import akka.stream.scaladsl.Flow
import spekka.context.FlowWithExtendedContext
import spekka.context.Partition
import spekka.context.PartitionDynamic

object PartitionDynamicBenchmark extends App {

  implicit val system = ActorSystem("partition-dynamic-benchmark")
  import FlowWithExtendedContext.syntax._

  def groupByFlow[Ctx](partitions: Int) = {
    val partitionFlow = Flow[(Int, Ctx)].map { case (v, ctx) => -v -> ctx }
    Flow[(Int, Ctx)]
      .groupBy(Int.MaxValue, _._1 % partitions)
      .via(partitionFlow)
      .mergeSubstreams
  }

  def groupByWithTerminationFlow[Ctx](partitions: Int) = {
    val partitionFlow = Flow[(Int, Ctx)].map { case (v, ctx) => -v -> ctx }
    Flow[(Int, Ctx)]
      .groupBy(Int.MaxValue, _._1 % partitions)
      .via(partitionFlow.take(partitions.toLong))
      .mergeSubstreams
  }

  def partitionDynamicFlow[Ctx](partitions: Int) =
    Partition.dynamic[Int, Int, Ctx, Int, Any](
      { case (v, _) => v % partitions },
      _ => FlowWithExtendedContext[Int, Ctx].map { v => -v },
      PartitionDynamic.CompletionCriteria.never
    )

  def partitionDynamicWithTerminationFlow[Ctx](partitions: Int) =
    Partition.dynamic[Int, Int, Ctx, Int, Any](
      { case (v, _) => v % partitions },
      _ => FlowWithExtendedContext[Int, Ctx].map { v => -v },
      PartitionDynamic.CompletionCriteria.onInput { v: Int =>
        val k = v % partitions

        v % (k + partitions) == 0
      }
    )

  def partitionDynamicMultiFlow[Ctx](partitions: Int) =
    Partition.dynamicMulti[Int, Int, Ctx, Int, Any](
      { case (v, _, _) => Set(v % partitions) },
      _ => FlowWithExtendedContext[Int, Ctx].map(v => -v),
      FlowWithExtendedContext[Int, Ctx].map(v => v),
      PartitionDynamic.CompletionCriteria.never
    )

  val partitions = 100

  val bGroupBy = StreamBenchmarkRunner.forGraph("groupBy", Iterator.from(0))(
    Flow[Int].zipWithIndex.via(groupByFlow[Long](partitions))
  )

  val bGroupByTermination = StreamBenchmarkRunner.forGraph("groupBy-termination", Iterator.from(0))(
    Flow[Int].zipWithIndex.via(groupByWithTerminationFlow[Long](partitions))
  )

  val bPartitionDynamicSingle =
    StreamBenchmarkRunner.forGraph("partitionDynamic-single", Iterator.from(0))(
      Flow[Int].zipWithIndex.via(partitionDynamicFlow[Long](partitions))
    )

  val bPartitionDynamicSingleTermination =
    StreamBenchmarkRunner.forGraph("partitionDynamic-single-termination", Iterator.from(0))(
      Flow[Int].zipWithIndex.via(partitionDynamicWithTerminationFlow[Long](partitions))
    )

  val bPartitionDynamicMulti =
    StreamBenchmarkRunner.forGraph("partitionDynamic-multi", Iterator.from(0))(
      Flow[Int].zipWithIndex.via(partitionDynamicMultiFlow[Long](partitions))
    )

  try {
    StreamBenchmarkRunner
      .runAll(1000000, 5)(
        bGroupBy,
        bGroupByTermination,
        bPartitionDynamicSingle,
        bPartitionDynamicSingleTermination,
        bPartitionDynamicMulti
      )
      .printResults()
  } catch {
    case ex: Exception =>
      ex.printStackTrace()
  } finally {
    system.terminate()
    ()
  }
}
