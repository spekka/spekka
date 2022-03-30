import akka.Done
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import spekka.context.FlowWithExtendedContext
import spekka.context.Partition
import spekka.context.PartitionTree

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._

object PartitionAutoBaseExample extends App {
  implicit val system = ActorSystem("context-partition-auto-standalone-example")
  import scala.concurrent.ExecutionContext.Implicits.global

  import PeopleEntranceCounterModel._

  // #flow-definition
  def entrancesSumFlow = {
    /*
     * The following is not safe to do, it is used in this example just as an
     * easy way to provide something in the materialization. Spekka offers
     * stateful flows which solves this problem in a cleaner way.
     */
    val ref = new AtomicReference[Int](0)
    FlowWithExtendedContext[CounterSample, Offset]
      .statefulMap { () =>
        var total: Int = 0

        counter =>
          total = total + counter.entrances
          ref.set(total)
          counter.timestamp -> total
      }
      .mapMaterializedValue(_ => ref)
  }
  // #flow-definition

  def printingFlow(name: String): FlowWithExtendedContext[(Long, Int), Unit, Offset, NotUsed] =
    FlowWithExtendedContext[(Long, Int), Offset].map { case (ts, total) =>
      println(s"$name - timestamp:${ts} counter:${total}")
      ()
    }

  val offsetCommittingSink: Sink[(Any, Offset), Future[Done]] =
    Sink.foreach(o => println(s"Committing offset ${o._2}"))

  // #partition-deployment
  import PartitionTree._
  val totalByDeploymentFlow = Partition
    .treeBuilder[CounterSample, Offset]
    .dynamicAuto(_.deploymentId)
    .build { case deployment :@: KNil =>
      entrancesSumFlow.via(printingFlow(s"deployment:${deployment.id} total"))
    }
  // #partition-deployment

  val samplesSource = readingsSource(30.seconds)(
    DeploymentSpec("a", 3, 1),
    DeploymentSpec("b", 2, 2)
  )

  // #stream-materialization
  val (control, done) = samplesSource
    .viaMat(totalByDeploymentFlow.ordered())(Keep.right)
    .toMat(offsetCommittingSink)(Keep.both)
    .run()
  // #stream-materialization

  // Request counter snapshots by accessing flow materialized values

  akka.pattern.after(15.seconds) {
    // #stream-query
    (for {
      aTotal <- control.atKey(DeploymentId("a"))
    } yield println(
      s"*** deployment a total ${aTotal.get()}"
    )).run
    // #stream-query
  }

  Await.result(done, Duration.Inf)

  system.terminate()
}