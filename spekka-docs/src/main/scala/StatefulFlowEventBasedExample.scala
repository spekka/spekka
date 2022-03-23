import akka.Done
import akka.NotUsed
import akka.actor.ActorSystem
import akka.actor.typed.ActorRef
import akka.pattern.StatusReply
import akka.stream.scaladsl.Keep
import akka.stream.scaladsl.Sink
import spekka.context.FlowWithExtendedContext
import spekka.context.Partition
import spekka.context.PartitionTree
import spekka.stateful.InMemoryStatefulFlowBackend
import spekka.stateful.StatefulFlowLogic
import spekka.stateful.StatefulFlowRegistry

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration._

object StatefulFlowEventBasedExample extends App {
  implicit val system = ActorSystem("stateful-event-example")
  import scala.concurrent.ExecutionContext.Implicits.global

  import PeopleEntranceCounterModel._

  val registry = StatefulFlowRegistry(30.seconds)

  case class CounterState(total: Int)
  case class IncrementCounter(c: Int)
  case class GetCounter(replyTo: ActorRef[StatusReply[Int]])

  import StatefulFlowLogic._
  val logic = EventBased[CounterState, IncrementCounter, CounterSample, GetCounter](
    () => CounterState(0),
    (state, sample) => {
      println(
        s"deployment:${sample.deploymentId} entrance:${sample.entranceId} - " +
          s"timestamp:${sample.timestamp} counter:${state.total + sample.entrances}"
      )
      EventBased.ProcessingResult.withEvent(IncrementCounter(sample.entrances))
    },
    (state, ev) => {
      val newTotal = state.total + ev.c
      CounterState(newTotal)
    },
    (state, command) => {
      command.replyTo ! StatusReply.success(state.total)
      EventBased.ProcessingResult.empty
    }
  )

  val backend = InMemoryStatefulFlowBackend.EventBased[CounterState, IncrementCounter]()

  val flowProps = logic.propsForBackend(backend)

  val byDeploymentFlowBuilder = registry.registerStatefulFlowSync("byDeployment", flowProps)
  val byEntranceFlowBuilder = registry.registerStatefulFlowSync("byEntrance", flowProps)

  def printingFlow(
      name: String
    ): FlowWithExtendedContext[Seq[IncrementCounter], Unit, Offset, NotUsed] =
    FlowWithExtendedContext[Seq[IncrementCounter], Offset].map { case increments =>
      println(s"$name - counter incremented by ${increments.map(_.c).sum}")
      ()
    }

  val offsetCommittingSink: Sink[(Any, Offset), Future[Done]] =
    Sink.foreach(o => println(s"Committing offset ${o._2}"))

  import PartitionTree._
  val totalByEntranceFlow = Partition
    .treeBuilder[CounterSample, Offset]
    .dynamicAuto(_.deploymentId)
    .dynamicAuto(_.entranceId)
    .build { case (entrance: EntranceId) :@: (deployment: DeploymentId) :@: KNil =>
      byEntranceFlowBuilder
        .flowWithExtendedContext(s"${deployment.id}:${entrance.id}")
        .via(printingFlow(s"deployment:${deployment.id} entrance:${entrance.id}"))
    }

  val totalByDeploymentFlow = Partition
    .treeBuilder[CounterSample, Offset]
    .dynamicAuto(_.deploymentId)
    .build { case deployment :@: KNil =>
      byDeploymentFlowBuilder
        .flowWithExtendedContext(s"${deployment.id}")
        .via(printingFlow(s"deployment:${deployment.id} total"))
    }

  sealed trait CombinedMaterialization
  object CombinedMaterialization {
    case class ByEntrance[M](
        control: PartitionControl.DynamicControl[
          DeploymentId,
          PartitionControl.DynamicControl[EntranceId, M]
        ])
    case class ByDeployment[M](
        control: PartitionControl.DynamicControl[DeploymentId, M])
  }
  val combinedFlow = Partition
    .treeBuilder[CounterSample, Offset]
    .staticMulticast(
      (_, keys: Set[String]) => keys,
      Set("byEntrance", "byDeployment")
    )
    .build {
      case "byEntrance" :@: KNil =>
        totalByEntranceFlow.mapMaterializedValue(CombinedMaterialization.ByEntrance(_))
      case "byDeployment" :@: KNil =>
        totalByDeploymentFlow.mapMaterializedValue(CombinedMaterialization.ByDeployment(_))
      case _ => ???
    }

  val (control, done) = readingsSource(30.seconds)(
    DeploymentSpec("a", 3, 1),
    DeploymentSpec("b", 2, 2)
  ).viaMat(combinedFlow.ordered())(Keep.right)
    .toMat(offsetCommittingSink)(Keep.both)
    .run()

  // Request counter snapshots by accessing flow materialized values
  akka.pattern.after(15.seconds) {
    for {
      byDeploymentC <- byDeploymentFlowBuilder.control("a")
      byEntranceC <- byEntranceFlowBuilder.control("a:1")
      aTotalF = byDeploymentC.get.commandWithResult(GetCounter)
      aE1TotalF = byEntranceC.get.commandWithResult(GetCounter)
      aTotal <- aTotalF
      aE1Total <- aE1TotalF
      _ = println(s"*** deployment a total ${aTotal}; deployment a entrance 1 total ${aE1Total}")
    } yield ()
  }

  Await.result(done, Duration.Inf)

  system.terminate()
}
