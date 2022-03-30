import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source

import scala.concurrent.Future
import scala.concurrent.duration._

/** Model objects used in examples
  */
object PeopleEntranceCounterModel {

  // #definitions
  /** Fake offset context type (i.e. equivalent of a Kakfa offset)
    */
  type Offset = Long

  /** The physical deployment where the sensors are deployed
    */
  case class DeploymentId(id: String)

  /** The id of the entrance where the sensor is located
    */
  case class EntranceId(id: Int)

  /** A sample produced by the People Counter sensor network detailing the number of entrances that
    * occurred since the last sample was sent
    */
  case class CounterSample(
      deploymentId: DeploymentId,
      entranceId: EntranceId,
      timestamp: Long,
      entrances: Int)

  // #definitions

  case class DeploymentSpec(id: String, entrancesNr: Int, entrancesPerSecond: Int)

  /** Helper function to generate readings for the specified deployments and entrances
    *
    * @param deploymentSpecs
    *   deployment specification (id, #entrances)
    * @param startTimestamp
    *   the start timestamp
    * @param duration
    *   duration of the generated samples
    * @return
    */
  def generateReadings(
      deploymentSpecs: List[DeploymentSpec],
      startTimestamp: Long,
      duration: FiniteDuration
    ): Iterator[CounterSample] = {
    for {
      ts <- Iterator
        .iterate(startTimestamp)(ts => ts + 1000)
        .takeWhile(_ < startTimestamp + duration.toMillis)
      dspec <- deploymentSpecs.iterator
      entrance <- Iterator.range(0, dspec.entrancesNr)
      value = dspec.entrancesPerSecond
    } yield (CounterSample(DeploymentId(dspec.id), EntranceId(entrance), ts, value))
  }

  /** Helper function to generate a counter samples source for the specified deployments and
    * entrances
    *
    * @param deploymentSpecs
    *   deployment specification (id, #entrances)
    * @param startTimestamp
    *   the start timestamp
    * @param duration
    *   duration of the generated samples
    * @return
    */
  def readingsSource(
      duration: FiniteDuration,
      startTimestamp: Long = 0
    )(deploymentSpecs: DeploymentSpec*
    )(implicit system: ActorSystem
    ): Source[(CounterSample, Offset), NotUsed] = {
    Source
      .fromIterator(() => generateReadings(deploymentSpecs.toList, startTimestamp, duration))
      .zipWithIndex
      .statefulMapConcat(() => {
        var lastTs = startTimestamp

        { case (r, o) =>
          if (r.timestamp > lastTs) {
            val delay = r.timestamp - lastTs
            lastTs = r.timestamp
            Some(akka.pattern.after(delay.millis)(Future.successful(r -> o)))
          } else Some(Future.successful(r -> o))
        }
      })
      .mapAsync(1)(identity)
  }
}
