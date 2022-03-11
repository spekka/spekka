package spekka.stateful

import akka.Done
import akka.actor.typed.ActorRef
import akka.pattern.StatusReply

import scala.concurrent.Future

trait StatefulFlowControl[Command] {

  def command(command: Command): Unit

  def commandWithResult[Result](f: ActorRef[StatusReply[Result]] => Command): Future[Result]

  def terminate(): Future[Done]
}
