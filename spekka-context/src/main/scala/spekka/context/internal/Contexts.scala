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

package spekka.context.internal

import spekka.context.StackableContext

/** Defines that a particular context having a hash equal to `hash` is multiplexed for `n` elements
  *
  * @param hash
  *   The hash of the original context
  * @param n
  *   The number of elements the context is multiplexed for
  */
private[spekka] case class MultiplexedContext(hash: Int, n: Int) extends StackableContext

/** Defines the sequence number associated to a particular context
  *
  * @param seqNr
  *   The sequence number associated to the context
  */
private[spekka] case class SequenceNumberContext(seqNr: Long) extends StackableContext
