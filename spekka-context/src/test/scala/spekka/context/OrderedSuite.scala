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

import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import spekka.test.SpekkaSuite

import scala.util.Failure
import scala.util.Success

class OrderedSuite extends SpekkaSuite("OrderedSuite") {

  import scala.concurrent.ExecutionContext.Implicits.global
  import FlowWithExtendedContext._

  def testOrder(batchSize: Int)(flow: FlowWithExtendedContext[Int, _, Long, _]) = {
    val source = Source.fromIterator(() => Iterator.from(0).take(200000)).zipWithIndex

    val checkSink = Sink.fold[Long, Long](0L) { case (expected, res) =>
      res shouldBe expected
      expected + 1
    }

    val done = source.via(flow.ordered(batchSize)).map(_._2).runWith(checkSink).andThen {
      case Success(_) =>
      case Failure(exception) => exception.printStackTrace()
    }

    done.futureValue
  }

  def reorderFlow[T](batchSize: Int) =
    Flow[T].statefulMapConcat(() => {
      val buff = scala.collection.mutable.ListBuffer[T]()

      in =>
        buff += in
        if (buff.size >= batchSize) {
          val res = buff.toList
          buff.clear()
          res.reverse
        } else Nil
    })

  test("ordered has no effect on already ordered sync flow") {
    testOrder(1)(FlowWithExtendedContext[Int, Long].map(_.toHexString))
  }

  test("ordered has no effect on already ordered async flow") {
    testOrder(1)(FlowWithExtendedContext[Int, Long].map(_.toHexString).async)
  }

  test("ordered enforce ordering on unordered sync flow with small batch size") {
    val batchSize = 10
    testOrder(batchSize) {
      FlowWithExtendedContext[Int, Long].toFlow
        .via(reorderFlow(batchSize))
        .asFlowWithExtendedContextUnsafe
    }
  }

  test("ordered enforce ordering on unordered sync flow with large batch size") {
    val batchSize = 500
    testOrder(batchSize) {
      FlowWithExtendedContext[Int, Long].toFlow
        .via(reorderFlow(batchSize))
        .asFlowWithExtendedContextUnsafe
    }
  }

  test("ordered enforce ordering on unordered async flow with small batch size") {
    val batchSize = 10
    testOrder(batchSize) {
      FlowWithExtendedContext[Int, Long].toFlow
        .via(reorderFlow(batchSize))
        .asFlowWithExtendedContextUnsafe
        .async
    }
  }

  test("ordered enforce ordering on unordered async flow with large batch size") {
    val batchSize = 500
    testOrder(batchSize) {
      FlowWithExtendedContext[Int, Long].toFlow
        .via(reorderFlow(batchSize))
        .asFlowWithExtendedContextUnsafe
        .async
    }
  }

  test("ordered enforce ordering for static multicast partitioned flows with small batch size") {
    val batchSize = 10

    val flow = FlowWithExtendedContext[Int, Long].async

    testOrder(batchSize) {
      Partition
        .treeBuilder[Int, Long]
        .staticMulticast[String]({ case (_, keys) => keys }, Set("a", "b", "c", "d"))
        .build { case _ => flow }
    }
  }

  test("ordered enforce ordering for static multicast partitioned flows with large batch size") {
    val batchSize = 500

    val flow = FlowWithExtendedContext[Int, Long].async

    testOrder(batchSize) {
      Partition
        .treeBuilder[Int, Long]
        .staticMulticast[String]({ case (_, keys) => keys }, Set("a", "b", "c", "d"))
        .build { case _ => flow }
    }
  }

  test("ordered enforce ordering for dynamic multicast partitioned flows with small batch size") {
    val batchSize = 10

    val flow = FlowWithExtendedContext[Int, Long].async

    testOrder(batchSize) {
      Partition
        .treeBuilder[Int, Long]
        .dynamicManualMulticast[String]({ case (_, keys) => keys }, Set("a", "b", "c", "d"))
        .build { case _ => flow }
    }
  }

  test("ordered enforce ordering for dynamic multicast partitioned flows with large batch size") {
    val batchSize = 500

    val flow = FlowWithExtendedContext[Int, Long].async

    testOrder(batchSize) {
      Partition
        .treeBuilder[Int, Long]
        .dynamicManualMulticast[String]({ case (_, keys) => keys }, Set("a", "b", "c", "d"))
        .build { case _ => flow }
    }
  }
}
