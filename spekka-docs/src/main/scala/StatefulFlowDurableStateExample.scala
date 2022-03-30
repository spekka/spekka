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

object StatefulFlowDurableStateExample extends App {
  implicit val system = ActorSystem("stateful-durable-state-example")
  import scala.concurrent.ExecutionContext.Implicits.global

  import PeopleEntranceCounterModel._

  val registry = StatefulFlowRegistry(30.seconds)

  // #definitions
  /** The state model */
  case class CounterState(total: Int)

  /** The command used to query the flow for its current counter value */
  case class GetCounter(replyTo: ActorRef[StatusReply[Int]])
  // #definitions

  // #logic
  import StatefulFlowLogic._
  val logic = DurableState[CounterState, CounterSample, Int, GetCounter](
    () => CounterState(0),
    (state, sample) => {
      val newCounter = state.total + sample.entrances
      println(
        s"deployment:${sample.deploymentId} entrance:${sample.entranceId} - " +
          s"timestamp:${sample.timestamp} counter:${newCounter}"
      )
      DurableState
        .ProcessingResult(CounterState(newCounter))
        .withOutput(newCounter)
    },
    (state, command) => {
      command.replyTo ! StatusReply.success(state.total)
      DurableState.ProcessingResult(state)
    }
  )
  // #logic

  // #backend
  val backend = InMemoryStatefulFlowBackend.DurableState[CounterState]()
  // #backend

  // #props
  val flowProps = logic.propsForBackend(backend)
  // #props

  // #registration
  val byDeploymentFlowBuilder = registry.registerStatefulFlowSync("byDeployment", flowProps)
  val byEntranceFlowBuilder = registry.registerStatefulFlowSync("byEntrance", flowProps)
  // #registration

  def printingFlow(name: String): FlowWithExtendedContext[Seq[Int], Unit, Offset, NotUsed] =
    FlowWithExtendedContext[Seq[Int], Offset].map { case counters =>
      println(s"$name - counter:${counters.max}")
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
