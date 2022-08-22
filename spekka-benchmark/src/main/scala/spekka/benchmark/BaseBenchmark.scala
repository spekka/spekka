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
import akka.stream.scaladsl.FlowWithContext
import spekka.context.FlowWithExtendedContext

object BaseBenchmark extends App {

  implicit val system = ActorSystem("stage-communication")

  val buffer: Int = 100

  val flow = StreamBenchmarkRunner.forGraph("flow", Iterator.from(0))(
    Flow[Int].map(_ * 2)
  )
  val flowCtx =
    StreamBenchmarkRunner.forGraph("flow-with-ctx", Iterator.from(0).zipWithIndex)(
      FlowWithContext[Int, Int].map(_ * 2)
    )

  val flowExtCtx =
    StreamBenchmarkRunner.forGraph("flow-with-ext-ctx", Iterator.from(0).zipWithIndex)(
      FlowWithExtendedContext[Int, Int].map(_ * 2)
    )

  val flowExtCtxOrdered =
    StreamBenchmarkRunner.forGraph("flow-with-ext-ctx-ordered", Iterator.from(0).zipWithIndex)(
      FlowWithExtendedContext[Int, Int].map(_ * 2).ordered()
    )

  try {
    StreamBenchmarkRunner
      .runAll(1000000, 5)(
        flow,
        flowCtx,
        flowExtCtx,
        flowExtCtxOrdered
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
