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

import akka.NotUsed
import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.FlowWithContext
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import spekka.test.SpekkaSuite

class FlowWithExtendedContextSuite
    extends SpekkaSuite(
      "FlowWithExtendedContextSuite"
    ) {

  test("Connection to Source") {
    val preservedContextFlow = FlowWithExtendedContext[String, Int]
      .map(s => s"$s-done")

    val inputs = List("one", "two", "three").zipWithIndex
    val expected = inputs.map { case (s, i) => s"$s-done" -> i }

    val sinkProbe = Source(inputs)
      .via(preservedContextFlow)
      .runWith(TestSink.probe)

    sinkProbe
      .request(inputs.size + 1L)
      .expectNextN(expected)
      .expectComplete()
  }

  test("Connection to Flow") {
    val preservedContextFlow = FlowWithExtendedContext[String, Int]
      .map(s => s"$s-done")

    val inputs = List("one", "two", "three").zipWithIndex
    val expected = inputs.map { case (s, i) => s"$s-done" -> i }

    val flow = Flow[(String, Int)].via(preservedContextFlow)

    val sinkProbe = Source(inputs)
      .via(flow)
      .runWith(TestSink.probe)

    sinkProbe
      .request(inputs.size + 1L)
      .expectNextN(expected)
      .expectComplete()
  }

  test("Connection to FlowWithContext") {
    val preservedContextFlow = FlowWithExtendedContext[String, Int]
      .map(s => s"$s-done")

    val inputs = List("one", "two", "three").zipWithIndex
    val expected = inputs.map { case (s, i) => s"$s-done" -> i }

    val flow = FlowWithContext[String, Int].via(preservedContextFlow)

    val sinkProbe = Source(inputs)
      .via(flow)
      .runWith(TestSink.probe)

    sinkProbe
      .request(inputs.size + 1L)
      .expectNextN(expected)
      .expectComplete()
  }

  test("Conversion to Flow and back") {
    import FlowWithExtendedContext.syntax._
    val preservedContextFlow: FlowWithExtendedContext[String, String, Int, NotUsed] =
      FlowWithExtendedContext[String, Int]
        .map(s => s"$s-done")
        .toFlow
        .map { case (s, i) => s"${s}F" -> i }
        .asFlowWithExtendedContextUnsafe

    val inputs = List("one", "two", "three").zipWithIndex
    val expected = inputs.map { case (s, i) => s"$s-doneF" -> i }

    val flow = FlowWithContext[String, Int].via(preservedContextFlow)

    val sinkProbe = Source(inputs)
      .via(flow)
      .runWith(TestSink.probe)

    sinkProbe
      .request(inputs.size + 1L)
      .expectNextN(expected)
      .expectComplete()
  }

  test("Conversion to FlowWithContext and back") {
    import FlowWithExtendedContext.syntax._
    val preservedContextFlow: FlowWithExtendedContext[String, String, Int, NotUsed] =
      FlowWithExtendedContext[String, Int]
        .map(s => s"$s-done")
        .toFlowWithContext
        .map(s => s"${s}FC")
        .asFlowWithExtendedContextUnsafe

    val inputs = List("one", "two", "three").zipWithIndex
    val expected = inputs.map { case (s, i) => s"$s-doneFC" -> i }

    val flow = FlowWithContext[String, Int].via(preservedContextFlow)

    val sinkProbe = Source(inputs)
      .via(flow)
      .runWith(TestSink.probe)

    sinkProbe
      .request(inputs.size + 1L)
      .expectNextN(expected)
      .expectComplete()
  }

  test("Zip of 2 FlowWithExtendedContext") {
    import FlowWithExtendedContext.syntax._
    case class Out1(s: String)
    case class Out2(s: String)

    val flow1 = FlowWithExtendedContext[String, Int]
      .map { in =>
        Out1(s"${in}-1")
      }
      .mapMaterializedValue(_ => 1)

    val flow2 = FlowWithExtendedContext[String, Int]
      .map { in =>
        Out2(s"${in}-2")
      }
      .mapMaterializedValue(_ => 2)

    val testFlow = FlowWithExtendedContext.zip(flow1, flow2)

    val inputs = List("one", "two", "three").zipWithIndex
    val expected = inputs.map { case (s, i) =>
      val outs = (Out1(s"$s-1"), Out2(s"$s-2"))
      outs -> i
    }

    val expectedMats = (1, 2)

    val (mats, sinkProbe) = Source(inputs)
      .viaMat(testFlow)(Keep.right)
      .toMat(TestSink.probe)(Keep.both)
      .run()

    mats shouldEqual expectedMats

    sinkProbe
      .request(inputs.size + 1L)
      .expectNextN(expected)
      .expectComplete()
  }

  test("Zip of 3 FlowWithExtendedContext") {
    import FlowWithExtendedContext.syntax._
    case class Out1(s: String)
    case class Out2(s: String)
    case class Out3(s: String)

    val flow1 = FlowWithExtendedContext[String, Int]
      .map { in =>
        Out1(s"${in}-1")
      }
      .mapMaterializedValue(_ => 1)

    val flow2 = FlowWithExtendedContext[String, Int]
      .map { in =>
        Out2(s"${in}-2")
      }
      .mapMaterializedValue(_ => 2)

    val flow3 = FlowWithExtendedContext[String, Int]
      .map { in =>
        Out3(s"${in}-3")
      }
      .mapMaterializedValue(_ => 3)

    val testFlow = FlowWithExtendedContext.zip(flow1, flow2, flow3)

    val inputs = List("one", "two", "three").zipWithIndex
    val expected = inputs.map { case (s, i) =>
      val outs = (Out1(s"$s-1"), Out2(s"$s-2"), Out3(s"$s-3"))
      outs -> i
    }

    val expectedMats = (1, 2, 3)

    val (mats, sinkProbe) = Source(inputs)
      .viaMat(testFlow)(Keep.right)
      .toMat(TestSink.probe)(Keep.both)
      .run()

    mats shouldEqual expectedMats

    sinkProbe
      .request(inputs.size + 1L)
      .expectNextN(expected)
      .expectComplete()
  }

  test("Zip of 4 FlowWithExtendedContext") {
    import FlowWithExtendedContext.syntax._
    case class Out1(s: String)
    case class Out2(s: String)
    case class Out3(s: String)
    case class Out4(s: String)

    val flow1 = FlowWithExtendedContext[String, Int]
      .map { in =>
        Out1(s"${in}-1")
      }
      .mapMaterializedValue(_ => 1)

    val flow2 = FlowWithExtendedContext[String, Int]
      .map { in =>
        Out2(s"${in}-2")
      }
      .mapMaterializedValue(_ => 2)

    val flow3 = FlowWithExtendedContext[String, Int]
      .map { in =>
        Out3(s"${in}-3")
      }
      .mapMaterializedValue(_ => 3)

    val flow4 = FlowWithExtendedContext[String, Int]
      .map { in =>
        Out4(s"${in}-4")
      }
      .mapMaterializedValue(_ => 4)

    val testFlow = FlowWithExtendedContext.zip(flow1, flow2, flow3, flow4)

    val inputs = List("one", "two", "three").zipWithIndex
    val expected = inputs.map { case (s, i) =>
      val outs = (Out1(s"$s-1"), Out2(s"$s-2"), Out3(s"$s-3"), Out4(s"$s-4"))
      outs -> i
    }

    val expectedMats = (1, 2, 3, 4)

    val (mats, sinkProbe) = Source(inputs)
      .viaMat(testFlow)(Keep.right)
      .toMat(TestSink.probe)(Keep.both)
      .run()

    mats shouldEqual expectedMats

    sinkProbe
      .request(inputs.size + 1L)
      .expectNextN(expected)
      .expectComplete()
  }

  test("Zip of 5 FlowWithExtendedContext") {
    import FlowWithExtendedContext.syntax._
    case class Out1(s: String)
    case class Out2(s: String)
    case class Out3(s: String)
    case class Out4(s: String)
    case class Out5(s: String)

    val flow1 = FlowWithExtendedContext[String, Int]
      .map { in =>
        Out1(s"${in}-1")
      }
      .mapMaterializedValue(_ => 1)

    val flow2 = FlowWithExtendedContext[String, Int]
      .map { in =>
        Out2(s"${in}-2")
      }
      .mapMaterializedValue(_ => 2)

    val flow3 = FlowWithExtendedContext[String, Int]
      .map { in =>
        Out3(s"${in}-3")
      }
      .mapMaterializedValue(_ => 3)

    val flow4 = FlowWithExtendedContext[String, Int]
      .map { in =>
        Out4(s"${in}-4")
      }
      .mapMaterializedValue(_ => 4)

    val flow5 = FlowWithExtendedContext[String, Int]
      .map { in =>
        Out5(s"${in}-5")
      }
      .mapMaterializedValue(_ => 5)

    val testFlow = FlowWithExtendedContext.zip(flow1, flow2, flow3, flow4, flow5)

    val inputs = List("one", "two", "three").zipWithIndex
    val expected = inputs.map { case (s, i) =>
      val outs = (Out1(s"$s-1"), Out2(s"$s-2"), Out3(s"$s-3"), Out4(s"$s-4"), Out5(s"$s-5"))
      outs -> i
    }

    val expectedMats = (1, 2, 3, 4, 5)

    val (mats, sinkProbe) = Source(inputs)
      .viaMat(testFlow)(Keep.right)
      .toMat(TestSink.probe)(Keep.both)
      .run()

    mats shouldEqual expectedMats

    sinkProbe
      .request(inputs.size + 1L)
      .expectNextN(expected)
      .expectComplete()
  }
}
