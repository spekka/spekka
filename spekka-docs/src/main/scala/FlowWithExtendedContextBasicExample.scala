import akka.Done
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import spekka.context.FlowWithExtendedContext

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._
import akka.stream.scaladsl.Source

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
  val done: Future[Done] = sampleSource.via(entrancesSumFlow)
    .via(printingFlow("total"))
    .runWith(offsetCommittingSink)
  // #flow-composition

  Await.result(done, Duration.Inf)

  system.terminate()
}
