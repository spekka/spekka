package spekka.stateful

import akka.stream.scaladsl.Flow
import akka.stream.scaladsl.FlowWithContext

import scala.concurrent.Future

trait StatefulFlowBuilder[In, Out, Command] {
  val entityKind: String

  def flow(entityId: String): Flow[In, Seq[Out], Future[StatefulFlowControl[Command]]]

  def flowWithContext[Ctx](
      entityId: String
    ): FlowWithContext[In, Ctx, Seq[Out], Ctx, Future[StatefulFlowControl[Command]]]

  def control(entityId: String): Future[Option[StatefulFlowControl[Command]]]
}
