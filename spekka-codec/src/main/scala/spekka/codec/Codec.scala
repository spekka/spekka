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

package spekka.codec

import scala.util.Try

sealed trait Encoder[T] {
  def encode(data: T): Array[Byte]
}
object Encoder {
  def apply[T](encoder: T => Array[Byte]): Encoder[T] =
    new Encoder[T] {
      override def encode(data: T): Array[Byte] = encoder(data)
    }
}

sealed trait Decoder[T] {
  def decode(bytes: Array[Byte]): Try[T]
}
object Decoder {
  def apply[T](decoder: Array[Byte] => Try[T]): Decoder[T] =
    new Decoder[T] {
      override def decode(bytes: Array[Byte]): Try[T] = decoder(bytes)
    }
}

final case class Codec[T](encoder: Encoder[T], decoder: Decoder[T])
object Codec {
  implicit def codecFromEncoderAndDecoder[T](
      implicit encoder: Encoder[T],
      decoder: Decoder[T]
    ): Codec[T] = Codec(encoder, decoder)
}
