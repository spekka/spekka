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

package spekka.context.internal

import akka.stream.FlowShape
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.GraphDSL
import akka.stream.scaladsl.Merge
import akka.stream.scaladsl.Partition
import spekka.context.ExtendedContext
import spekka.context.StackableContext

import scala.collection.immutable

private[spekka] object PartitionStaticInternal {
  case class PartitionedContext[K](partitionKey: Option[K]) extends StackableContext

  def buildSingle[In, Out, Ctx, K](
      keyF: (In, ExtendedContext[Ctx]) => K,
      flowByKey: Map[K, FlowShape[(In, ExtendedContext[Ctx]), (Out, ExtendedContext[Ctx])]],
      unknownKeysFlow: FlowShape[(In, ExtendedContext[Ctx]), (Out, ExtendedContext[Ctx])]
    )(implicit builder: GraphDSL.Builder[_]
    ): FlowShape[(In, ExtendedContext[Ctx]), (Out, ExtendedContext[Ctx])] = {
    import GraphDSL.Implicits._

    val flowDetailsSeq =
      flowByKey.iterator.zipWithIndex.map { case ((k, flow), idx) => k -> (idx -> flow) }.toList
    val flowDetailsByKey = flowDetailsSeq.toMap

    def partitioner(m: (In, ExtendedContext[Ctx])): Int =
      flowDetailsByKey.get(keyF(m._1, m._2)) match {
        case Some((idx, _)) => idx
        case None => flowByKey.size
      }

    val partition = builder.add(
      Partition[(In, ExtendedContext[Ctx])](flowByKey.size + 1, partitioner)
    )

    val merge = builder.add(
      Merge[(Out, ExtendedContext[Ctx])](
        flowByKey.size + 1,
        eagerComplete = false
      )
    )

    flowDetailsSeq.zip(partition.outlets.zip(merge.inlets)).foreach {
      case ((_, (_, flow)), (out, in)) =>
        out ~> flow ~> in
    }

    partition.outlets.last ~> unknownKeysFlow ~> merge.inlets.last

    FlowShape(partition.in, merge.out)
  }

  def buildMulti[In, Out, Ctx, K](
      keyF: (In, Ctx, Set[K]) => Set[K],
      flowByKey: Map[K, FlowShape[(In, ExtendedContext[Ctx]), (Out, ExtendedContext[Ctx])]],
      unknownKeysFlow: FlowShape[(In, ExtendedContext[Ctx]), (Out, ExtendedContext[Ctx])]
    )(implicit builder: GraphDSL.Builder[_]
    ): FlowShape[(In, ExtendedContext[Ctx]), (immutable.Iterable[Out], ExtendedContext[Ctx])] = {
    import GraphDSL.Implicits._

    def partitioner(ctx: ExtendedContext[Ctx]): Option[K] = {
      val pCtx = ctx
        .peek[PartitionedContext[K]]
        .getOrElse(
          throw new IllegalStateException(
            "Inconsistency in PartitionedFlow! Expected PartitionedContext to be top of the stack"
          )
        )
      pCtx.partitionKey
    }

    val preprocessingFlow = builder.add(Flow[(In, ExtendedContext[Ctx])].statefulMapConcat { () =>
      var globalSeqNr = 0L

      { case (in, ctx) =>
        val outPartitions = keyF(in, ctx.innerContext, flowByKey.keySet)
        globalSeqNr += 1

        outPartitions.size match {
          case 0 =>
            val outCtx = ctx
              .push(MultiplexedContext(globalSeqNr, 1))
              .push(PartitionedContext(None))
            List(in -> outCtx)

          case 1 =>
            val outCtx = ctx
              .push(MultiplexedContext(globalSeqNr, 1))
              .push(PartitionedContext(outPartitions.headOption))
            List(in -> outCtx)

          case n =>
            val baseOutCtx = ctx.push(MultiplexedContext(globalSeqNr, n))

            outPartitions.iterator.map { k =>
              val outCtx = baseOutCtx.push(PartitionedContext(Some(k)))
              in -> outCtx
            }.toList
        }
      }
    })

    val contextPassthroughFlow = builder.add(
      Flow[(In, ExtendedContext[Ctx])]
        .map { case (_, ctx) =>
          null.asInstanceOf[Out] -> ctx
        }
    )

    val extendedFlowMap
        : Map[Option[K], FlowShape[(In, ExtendedContext[Ctx]), (Out, ExtendedContext[Ctx])]] =
      flowByKey.map { case (key, flow) =>
        (Some(key): Option[K]) -> flow
      } ++ Map(None -> contextPassthroughFlow)

    val postProcessingFlow = builder.add(
      Flow[(Out, ExtendedContext[Ctx])].map { case (out, ctx) =>
        out -> ctx.pop[PartitionedContext[K]]._1
      }
    )

    val partitionedFlow = buildSingle[In, Out, Ctx, Option[K]](
      (_: In, ctx: ExtendedContext[Ctx]) => partitioner(ctx),
      extendedFlowMap,
      unknownKeysFlow
    )

    val demultiplexFlow =
      builder.add(
        new Multiplexed.UnorderedMultiplexStage[Out, Ctx]
      )

    preprocessingFlow ~> partitionedFlow ~> postProcessingFlow ~> demultiplexFlow

    FlowShape(preprocessingFlow.in, demultiplexFlow.out)
  }
}
