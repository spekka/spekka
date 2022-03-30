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
