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
