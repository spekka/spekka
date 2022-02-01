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
import akka.stream.scaladsl.FlowWithContext
import akka.stream.scaladsl.Source
import akka.stream.testkit.scaladsl.TestSink
import spekka.stream.StreamSuite

class FlowWithExtendedContextSuite
    extends StreamSuite(
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
}
