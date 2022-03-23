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

object PartitionManualExample extends App {
  implicit val system = ActorSystem("context-partition-manual-example")
  import scala.concurrent.ExecutionContext.Implicits.global

  import PeopleEntranceCounterModel._

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

  def printingFlow(name: String): FlowWithExtendedContext[(Long, Int), Unit, Offset, NotUsed] =
    FlowWithExtendedContext[(Long, Int), Offset].map { case (ts, total) =>
      println(s"$name - timestamp:${ts} counter:${total}")
      ()
    }

  val offsetCommittingSink: Sink[(Any, Offset), Future[Done]] =
    Sink.foreach(o => println(s"Committing offset ${o._2}"))

  import PartitionTree._
  val totalByEntranceFlow = Partition
    .treeBuilder[CounterSample, Offset]
    .dynamicManual(_.deploymentId, Set.empty)
    .dynamicAuto(_.entranceId)
    .build { case (entrance: EntranceId) :@: (deployment: DeploymentId) :@: KNil =>
      entrancesSumFlow.via(printingFlow(s"deployment:${deployment.id} entrance:${entrance.id}"))
    }

  val totalByDeploymentFlow = Partition
    .treeBuilder[CounterSample, Offset]
    .dynamicManual(_.deploymentId, Set.empty)
    .build { case deployment :@: KNil =>
      entrancesSumFlow.via(printingFlow(s"deployment:${deployment.id} total"))
    }

  sealed trait CombinedMaterialization
  object CombinedMaterialization {
    case class ByEntrance(
        control: PartitionControl.DynamicControl[
          DeploymentId,
          PartitionControl.DynamicControl[EntranceId, AtomicReference[Int]]
        ])
    case class ByDeployment(
        control: PartitionControl.DynamicControl[DeploymentId, AtomicReference[Int]])
  }
  val combinedFlow = Partition
    .treeBuilder[CounterSample, Offset]
    .staticMulticast(
      (_, keys: Set[String]) => keys,
      Set("byEntrance", "byDeployment")
    )
    .build {
      case "byEntrance" :@: KNil =>
        totalByEntranceFlow.mapMaterializedValue(CombinedMaterialization.ByEntrance)
      case "byDeployment" :@: KNil =>
        totalByDeploymentFlow.mapMaterializedValue(CombinedMaterialization.ByDeployment)
      case _ => ???
    }

  val (control, done) = readingsSource(30.seconds)(
    DeploymentSpec("a", 3, 1),
    DeploymentSpec("b", 2, 2)
  ).viaMat(combinedFlow.ordered())(Keep.right)
    .toMat(offsetCommittingSink)(Keep.both)
    .run()

  def startProcessingDeployment(d: String): PartitionTree.PartitionControl.ControlResult[Unit] = {
    for {
      byDeploymentC <- control.atKeyNarrowed[CombinedMaterialization.ByDeployment]("byDeployment")
      byEntranceC <- control.atKeyNarrowed[CombinedMaterialization.ByEntrance]("byEntrance")
      _ <- byDeploymentC.get.control.materializeKey(DeploymentId(d))
      _ <- byEntranceC.get.control.materializeKey(DeploymentId(d))
    } yield ()
  }

  // Start processing deployment a after 5 seconds
  akka.pattern.after(5.seconds) {
    startProcessingDeployment("a").run
  }

  // Start processing deployment b after 10 seconds
  akka.pattern.after(10.seconds) {
    startProcessingDeployment("b").run
  }

  // Request counter snapshots by accessing flow materialized values
  akka.pattern.after(15.seconds) {
    (
      for {
        byDeploymentC <- control.atKeyNarrowed[CombinedMaterialization.ByDeployment]("byDeployment")
        aTotal <- byDeploymentC.get.control.atKey(DeploymentId("a"))
        byEntranceC <- control.atKeyNarrowed[CombinedMaterialization.ByEntrance]("byEntrance")
        aC <- byEntranceC.get.control.atKey(DeploymentId("a"))
        aE1Total <- aC.atKey(EntranceId(1))
      } yield println(
        s"*** deployment a total ${aTotal}; deployment a entrance 1 total ${aE1Total}"
      )
    ).run
  }

  Await.result(done, Duration.Inf)

  system.terminate()
}
