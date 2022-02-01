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

package spekka.stream

import akka.actor.ActorSystem
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.Millis
import org.scalatest.time.Seconds
import org.scalatest.time.Span

abstract class StreamSuite(_system: ActorSystem)
    extends AnyFunSuite
    with BeforeAndAfterAll
    with Matchers
    with ScalaFutures
    with TypeCheckedTripleEquals {

  def this(name: String, config: Config) = {
    this(ActorSystem(name, config))
  }

  def this(name: String, configString: String) = {
    this(name, ConfigFactory.parseString(configString).withFallback(ConfigFactory.load()))
  }

  def this(name: String) = {
    this(name, ConfigFactory.load())
  }

  implicit val system: ActorSystem = _system
  implicit val patience: PatienceConfig = PatienceConfig(Span(30, Seconds), Span(100, Millis))

  override protected def afterAll(): Unit = {
    system.terminate()
    ()
  }
}
