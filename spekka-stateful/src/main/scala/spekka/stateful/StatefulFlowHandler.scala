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

import akka.Done
import akka.actor.ExtendedActorSystem
import akka.actor.typed.ActorRef
import akka.actor.typed.ActorRefResolver
import akka.actor.typed.ActorSystem
import akka.pattern.StatusReply
import akka.serialization.SerializationExtension
import akka.serialization.SerializerWithStringManifest
import akka.serialization.Serializers

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import scala.util.Try

private[spekka] object StatefulFlowHandler {
  sealed trait Protocol[+In, -Out, +Command, -B]

  case class ProcessFlowInput[In, Out, Pass](
      in: In,
      passthrough: Pass,
      replyTo: ActorRef[StatusReply[(Seq[Out], Pass)]])
      extends Protocol[In, Out, Nothing, Any]

  case class ProcessCommand[Command](
      command: Command)
      extends Protocol[Nothing, Any, Command, Any]

  case class TerminateRequest(replyTo: ActorRef[StatusReply[Done]])
      extends Protocol[Nothing, Any, Nothing, Any]

  trait BackendProtocol[B] extends Protocol[Nothing, Any, Nothing, B]
}

class StatefulFlowHandlerProtocolSerializer(system: ExtendedActorSystem)
    extends SerializerWithStringManifest {
  private val actorRefResolver = ActorRefResolver(ActorSystem.wrap(system))
  private lazy val serialization = SerializationExtension(system)

  override def identifier: Int = 1003094656

  override def manifest(o: AnyRef): String =
    o match {
      case _: StatefulFlowHandler.ProcessFlowInput[_, _, _] => "flow-input"
      case _: StatefulFlowHandler.ProcessCommand[_] => "command"
      case _: StatefulFlowHandler.TerminateRequest => "terminate"
      case _ =>
        throw new IllegalArgumentException(
          s"Can't serialize object of type ${o.getClass} in [${getClass.getName}]"
        )
    }

  private def serializeObj(o: AnyRef): Either[Throwable, Array[Byte]] = {
    (for {
      oSerializer <- Try(serialization.findSerializerFor(o.asInstanceOf[AnyRef])).toEither
      oBytes <- Try(oSerializer.toBinary(o.asInstanceOf[AnyRef])).toEither
      oManifest <- Try(Serializers.manifestFor(oSerializer, o.asInstanceOf[AnyRef])).toEither
      oManifestBytes = oManifest.getBytes(StandardCharsets.UTF_8)
      oSerializerId = oSerializer.identifier
      serBytes = {
        val buff = ByteBuffer.allocate(4 + 8 + oManifest.size + oBytes.size)
        buff
          .putInt(oSerializerId)
          .putInt(oManifest.size)
          .putInt(oBytes.size)
          .put(oManifestBytes)
          .put(oBytes)
          .array()
      }
    } yield serBytes)
  }

  private def deserializeObj(bytes: Array[Byte]): AnyRef = {
    val buff = ByteBuffer.wrap(bytes)

    val serializerId = buff.getInt()
    val manifestSize = buff.getInt()
    val objSize = buff.getInt()
    val manifestBytes = Array.ofDim[Byte](manifestSize)
    val objBytes = Array.ofDim[Byte](objSize)
    buff.get(manifestBytes)
    buff.get(objBytes)

    val manifest = new String(manifestBytes, StandardCharsets.UTF_8)
    serialization.deserialize(objBytes, serializerId, manifest).fold(throw _, identity)
  }

  override def toBinary(o: AnyRef): Array[Byte] =
    o match {
      case StatefulFlowHandler.ProcessFlowInput(in, pass, replyTo) =>
        (for {
          inBytes <- serializeObj(in.asInstanceOf[AnyRef])
          passBytes <- serializeObj(pass.asInstanceOf[AnyRef])
          actorRefBytes = actorRefResolver
            .toSerializationFormat(replyTo)
            .getBytes(StandardCharsets.UTF_8)
          serBytes = {
            val buff = ByteBuffer.allocate(inBytes.size + passBytes.size + actorRefBytes.size + 12)
            buff
              .putInt(inBytes.size)
              .putInt(passBytes.size)
              .putInt(actorRefBytes.size)
              .put(inBytes)
              .put(passBytes)
              .put(actorRefBytes)

            buff.array()
          }
        } yield serBytes).fold(
          err =>
            throw new IllegalArgumentException(s"Error serializing ${o.getClass().getName()}", err),
          identity
        )

      case StatefulFlowHandler.ProcessCommand(command) =>
        serializeObj(command.asInstanceOf[AnyRef]).fold(
          err =>
            throw new IllegalArgumentException(s"Error serializing ${o.getClass().getName()}", err),
          identity
        )

      case StatefulFlowHandler.TerminateRequest(replyTo) =>
        actorRefResolver.toSerializationFormat(replyTo).getBytes(StandardCharsets.UTF_8)

      case _ =>
        throw new IllegalArgumentException(
          s"Can't serialize object of type ${o.getClass} in [${getClass.getName}]"
        )
    }

  override def fromBinary(bytes: Array[Byte], manifest: String): AnyRef =
    manifest match {
      case "flow-input" =>
        try {
          val buff = ByteBuffer.wrap(bytes)

          val inBytesLength = buff.getInt()
          val passBytesLength = buff.getInt()
          val actorRefBytesLength = buff.getInt()

          val inBytes = Array.ofDim[Byte](inBytesLength)
          val passBytes = Array.ofDim[Byte](passBytesLength)
          val actorRefBytes = Array.ofDim[Byte](actorRefBytesLength)

          buff.get(inBytes)
          buff.get(passBytes)
          buff.get(actorRefBytes)

          val in = deserializeObj(inBytes)
          val pass = deserializeObj(passBytes)
          val actorRef =
            actorRefResolver.resolveActorRef(new String(actorRefBytes, StandardCharsets.UTF_8))

          StatefulFlowHandler.ProcessFlowInput(in, pass, actorRef)
        } catch {
          case e: Exception =>
            throw new IllegalArgumentException(
              s"Error de-serializing object of type ${classOf[StatefulFlowHandler.ProcessFlowInput[_, _, _]]
                .getName()}",
              e
            )
        }

      case "command" =>
        try {
          val command = deserializeObj(bytes)
          StatefulFlowHandler.ProcessCommand(command)
        } catch {
          case e: Exception =>
            throw new IllegalArgumentException(
              s"Error de-serializing object of type ${classOf[StatefulFlowHandler.ProcessCommand[_]].getName()}",
              e
            )
        }

      case "terminate" =>
        try {
          val actorRef =
            actorRefResolver.resolveActorRef(new String(bytes, StandardCharsets.UTF_8))
          StatefulFlowHandler.TerminateRequest(actorRef)
        } catch {
          case e: Exception =>
            throw new IllegalArgumentException(
              s"Error de-serializing object of type ${classOf[StatefulFlowHandler.ProcessCommand[_]].getName()}",
              e
            )
        }
    }
}
