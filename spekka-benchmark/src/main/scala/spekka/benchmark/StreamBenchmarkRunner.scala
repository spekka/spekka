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

import akka.stream.FlowShape
import akka.stream.Graph
import akka.stream.Materializer
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.RunnableGraph
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._

class StreamBenchmarkRunner[T](
    name: String,
    flow: Graph[FlowShape[T, _], _],
    valuesIt: () => Iterator[T],
    executionTimeout: Duration
  ) {

  def runWarmup(graph: RunnableGraph[Future[Long]])(implicit mat: Materializer): Unit = {
    println(s"[runner=${name}] running warmup")
    Await.result(graph.run(), executionTimeout)
    println(s"[runner=${name}] warmup completed")
  }

  def runIteration(
      graph: RunnableGraph[Future[Long]],
      i: Int
    )(implicit mat: Materializer
    ): (Long, Long) = {
    println(s"[runner=${name}] running iteration $i")

    val startTimestamp = System.currentTimeMillis()
    val outputsCount = Await.result(graph.run(), executionTimeout)
    val endTimestamp = System.currentTimeMillis()

    val duration = endTimestamp - startTimestamp
    println(s"[runner=${name}] ** duration=[${duration}]")
    duration -> outputsCount
  }

  def run(
      n: Int,
      iterations: Int = StreamBenchmarkRunner.defaultIterations
    )(implicit mat: Materializer
    ): StreamBenchmarkRunner.Result = {
    val graph = Source
      .fromIterator(valuesIt)
      .take(n.toLong)
      .via[Any, Any](flow)
      .toMat(Sink.fold(0L) { case (acc, _) => acc + 1 })(Keep.right)

    runWarmup(graph)
    val (times, outputCounts) = 1.to(iterations).map(runIteration(graph, _)).unzip
    val avgDuration = (times.sum.toDouble / times.size).round
    val stdDevDuration =
      Math.sqrt(times.map(x => Math.pow(x.toDouble - avgDuration, 2)).sum / times.size)

    val avgOutputSize = (outputCounts.sum.toDouble / outputCounts.size).round
    val stdDevOutputSize = Math.sqrt(
      outputCounts.map(x => Math.pow(x.toDouble - avgOutputSize, 2)).sum / outputCounts.size
    )
    StreamBenchmarkRunner.Result(
      name,
      iterations,
      n,
      outputCounts.min,
      outputCounts.max,
      avgOutputSize,
      stdDevOutputSize,
      times.min,
      times.max,
      avgDuration,
      stdDevDuration
    )
  }
}

object StreamBenchmarkRunner {
  val defaultExecutionTimeout: Duration = 5.minutes
  val defaultIterations: Int = 3

  case class Result(
      name: String,
      runs: Int,
      nIn: Int,
      nOutMin: Long,
      nOutMax: Long,
      nOutAvg: Long,
      nOutStdDev: Double,
      dMinMillis: Long,
      dMaxMillis: Long,
      dAvgMillis: Long,
      dStdDev: Double
    )

  case class ResultCollection(n: Int, inputSize: Int, results: Seq[Result]) {
    def printResults(): Unit = {
      val header =
        f"""Results for iterations=${n} input-size=${inputSize}
          |---------------------------------------------------
          |${"name"}%30s | ${"min-time"}%10s | ${"max-time"}%10s | ${"avg-time"}%10s | ${"std-dev-time"}%10s | ${"min-outs"}%10s | ${"max-outs"}%10s | ${"avg-outs"}%10s | ${"std-dev-outs"}%10s""".stripMargin

      def resultString(r: Result): String = {
        f"""${r.name.substring(
            0,
            math.min(r.name.size, 30)
          )}%-30s | ${r.dMinMillis}%10s | ${r.dMaxMillis}%10s | ${r.dAvgMillis}%10s | ${r.dStdDev}%-12.2f | ${r.nOutMin}%10s | ${r.nOutMax}%10s | ${r.nOutAvg}%10s | ${r.nOutStdDev}%-12.2f"""
      }
      println(header + "\n" + results.map(resultString).mkString("\n"))
    }
  }

  def runAll(
      n: Int,
      iterations: Int = defaultIterations
    )(benchs: StreamBenchmarkRunner[_]*
    )(implicit mat: Materializer
    ): ResultCollection = {
    ResultCollection(iterations, n, benchs.map(_.run(n, iterations)))
  }

  def forGraph[T](
      name: String,
      valuesIt: => Iterator[T],
      executionTimeout: Duration = defaultExecutionTimeout
    )(flow: Graph[FlowShape[T, _], _]
    ): StreamBenchmarkRunner[T] = {
    new StreamBenchmarkRunner[T](name, flow, () => valuesIt, executionTimeout)
  }
}
