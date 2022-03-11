package spekka.stateful

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

private[spekka] object SideEffectRunner {
  def run(sideEffects: Iterable[() => Future[_]])(implicit ec: ExecutionContext): Future[Unit] = {
    Future.sequence(sideEffects.map(_.apply())).map(_ => ())
  }

  def run(
      sideEffects: Iterable[() => Future[_]],
      parallelism: Int
    )(implicit ec: ExecutionContext
    ): Future[Unit] = {
    def go(groups: List[Iterable[() => Future[_]]]): Future[Unit] = {
      groups match {
        case Nil => Future.successful(())
        case g :: Nil => Future.sequence(g.map(_.apply())).map(_ => ())
        case g :: rest => Future.sequence(g.map(_.apply())).map(_ => ()).flatMap(_ => go(rest))
      }
    }
    go(sideEffects.grouped(parallelism).toList)
  }
}
