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

package spekka.context

import scala.reflect.ClassTag

/** Represent a context which can be extended with additional information without affecting the
  * original value
  */
trait ExtendedContext[CTX] {

  /** The original context that was made extensible
    */
  val innerContext: CTX

  /** Transform the original context
    *
    * @param f
    *   transform function
    * @return
    *   Extended context wrapping the new context value
    */
  def mapInnerContext[O](f: CTX => O): ExtendedContext[O]

  private[spekka] def peek[T <: StackableContext: ClassTag]: Option[T]
  private[spekka] def pop[T <: StackableContext: ClassTag]: (ExtendedContext[CTX], Option[T])
  private[spekka] def popOrElse[T <: StackableContext: ClassTag](
      default: => T
    ): (ExtendedContext[CTX], T)
  private[spekka] def push[T <: StackableContext](sc: T): ExtendedContext[CTX]
}

object ExtendedContext {

  /** Creates an extended context wrapping the base provided context
    *
    * @param context
    *   The context to make extensible
    * @return
    *   Extended context wrapping the base provided context
    */
  def apply[Ctx](context: Ctx): ExtendedContext[Ctx] = new ExtendedContextImpl(context, Nil)
}

private[spekka] trait StackableContext

private[spekka] case class ExtendedContextImpl[Ctx](
    val innerContext: Ctx,
    stack: List[StackableContext]
  ) extends ExtendedContext[Ctx] {

  override def mapInnerContext[O](f: Ctx => O): ExtendedContext[O] =
    copy(innerContext = f(innerContext))

  private def coerceContext[T <: StackableContext: ClassTag](
      ctx: Option[StackableContext]
    ): Option[T] =
    ctx.flatMap {
      case sc: T => Some(sc)
      case _ => None
    }

  override private[spekka] def peek[T <: StackableContext: ClassTag]: Option[T] =
    coerceContext(stack.headOption)

  override private[spekka] def pop[T <: StackableContext: ClassTag]
      : (ExtendedContext[Ctx], Option[T]) =
    peek[T] match {
      case res @ Some(_) => copy(stack = stack.tail) -> res
      case None => this -> None
    }

  override private[spekka] def popOrElse[T <: StackableContext: ClassTag](
      default: => T
    ): (ExtendedContext[Ctx], T) =
    pop[T] match {
      case (ctx, Some(sc)) => ctx -> sc
      case (ctx, None) => ctx -> default
    }

  override private[spekka] def push[T <: StackableContext](sc: T): ExtendedContext[Ctx] =
    copy(stack = sc :: stack)
}
