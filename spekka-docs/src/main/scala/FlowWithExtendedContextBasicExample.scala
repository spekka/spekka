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

import akka.Done
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import spekka.context.FlowWithExtendedContext

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._

object FlowWithExtendedContextBasicExample extends App {
  implicit val system = ActorSystem("context-basic-example")

  import PeopleEntranceCounterModel._

  // #flow-definition
  val entrancesSumFlow =
    FlowWithExtendedContext[CounterSample, Offset].statefulMap { () =>
      var total: Int = 0

      counter =>
        total = total + counter.entrances
        counter.timestamp -> total
    }
  // #flow-definition

  def printingFlow(name: String): FlowWithExtendedContext[(Long, Int), Unit, Offset, NotUsed] =
    FlowWithExtendedContext[(Long, Int), Offset].map { case (ts, total) =>
      println(s"$name - timestamp:${ts} counter:${total}")
      ()
    }

  val offsetCommittingSink: Sink[(Any, Offset), Future[Done]] =
    Sink.foreach(o => println(s"Committing offset ${o._2}"))

  val sampleSource: Source[(CounterSample, Offset), NotUsed] =
    readingsSource(30.seconds)(
      DeploymentSpec("a", 3, 1)
    )

  // #flow-composition
  val done: Future[Done] = sampleSource
    .via(entrancesSumFlow)
    .via(printingFlow("total"))
    .runWith(offsetCommittingSink)
  // #flow-composition

  Await.result(done, Duration.Inf)

  system.terminate()
}
