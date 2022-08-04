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

import akka.Done

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Try

object PartitionTree {

  /** Represents a typed sequence of keys in a partition tree.
    *
    * By construction, the order of the keys is the reversed w.r.t. the partitioning order.
    *
    * {{{
    * case class Input(k1: Int, k2: String, k3: Boolean)
    *
    * Partition.treeBuilder[Input, NotUsed]
    *   .dynamicAuto { case (in, _) => in.k1 }
    *   .dynamicAuto { case (in, _) => in.k2 }
    *   .dynamicAuto { case (in, _) => in.k3 }
    *   .build { case k3 :@: k2 :@: k1 :@: KNil =>
    *     FlowWithExtendedContext[Input, NotUsed].map { _ =>
    *       (k1, k2, k3)
    *     }
    *   }
    * }}}
    */
  sealed trait KSeq
  object KSeq {
    implicit class KSeqOpts[KS <: KSeq](kseq: KS) {
      def :@:[H](h: H): H :@: KS = new :@:(h, kseq)
    }
  }

  /** Constructor for key sequences.
    */
  final case class :@:[H, T <: KSeq](head: H, tail: T) extends KSeq

  /** Terminal element in a key sequence.
    */
  sealed trait KNil extends KSeq {
    def :@:[H](h: H): H :@: KNil = new :@:(h, this)
  }
  case object KNil extends KNil

  /** Base partition control object.
    *
    * Can be either a [[PartitionControl.DynamicControl]] or [[PartitionControl.StaticControl]].
    */
  sealed trait PartitionControl

  /** Partition control namespace object
    */
  object PartitionControl {

    /** Control for static partitions.
      */
    final class StaticControl[K, M] private[spekka] (layer: Map[K, M]) extends PartitionControl {

      /** Retrieve the materialization value of the partition with the specified key.
        *
        * @param k
        *   The partition key to retrieve the materialization of.
        * @return
        *   A control result containing the materialized value of the specified partition.
        */
      def atKey(k: K): ControlResult[M] = new ControlResult(_ => Future.successful(layer.get(k)))

      /** Retrieve the materialization value of the partition with the specified key, narrowing the
        * type of the materialization.
        *
        * In case the required type is not compatible with the actual materialization type, None is
        * returned.
        *
        * @param k
        *   The partition key to retrieve the materialization of.
        * @return
        *   A control result containing the materialized value of the specified partition.
        */
      def atKeyNarrowed[M1 <: M](k: K): ControlResult[Option[M1]] = {
        new ControlResult(_ =>
          Future.successful(
            layer.get(k).map { m =>
              Try(m.asInstanceOf[M1]).fold[Option[M1]](_ => None, m => Some(m))
            }
          )
        )
      }
    }

    /** Control for dynamic partitions.
      */
    final class DynamicControl[K, M] private[spekka] (layer: PartitionDynamic.Control[K, M])
        extends PartitionControl {

      /** Retrieve the materialization value of the partition with the specified key.
        *
        * @param k
        *   The partition key to retrieve the materialization of.
        * @return
        *   A control result containing the materialized value of the specified partition.
        */
      def atKey(k: K): ControlResult[M] =
        new ControlResult(_ => layer.withKeyMaterializedValue(k)(identity))

      /** Retrieve the materialization value of the partition with the specified key, narrowing the
        * type of the materialization.
        *
        * In case the required type is not compatible with the actual materialization type, None is
        * returned.
        *
        * @param k
        *   The partition key to retrieve the materialization of.
        * @return
        *   A control result containing the materialized value of the specified partition.
        */
      def atKeyNarrowed[M1 <: M](k: K): ControlResult[Option[M1]] = {
        new ControlResult(_ =>
          layer.withKeyMaterializedValue(k)(m => Try(m.asInstanceOf[M1]).fold(_ => None, Some(_)))
        )
      }

      /** Retrieve the materialization value of the partition with the specified key, forcing the
        * materialization of the partition in case it wasn't already materialized.
        *
        * @param k
        *   The partition key to retrieve the materialization of.
        * @return
        *   A control result containing the materialized value of the specified partition.
        */
      def atKeyForced(k: K): ControlResult[M] =
        new ControlResult((ec: ExecutionContext) =>
          layer
            .withKeyMaterializedValue(k)(identity)
            .flatMap {
              case Some(v) => Future.successful(Some(v))
              case None => layer.materializeKey(k).map(Some(_))(ec)
            }(ec)
        )

      /** Retrieve the materialization value of the partition with the specified key, forcing the
        * materialization of the partition in case it wasn't already materialized.
        *
        * In case the required type is not compatible with the actual materialization type, None is
        * returned.
        *
        * @param k
        *   The partition key to retrieve the materialization of.
        * @return
        *   A control result containing the materialized value of the specified partition.
        */
      def atKeyForcedNarrowed[M1](k: K): ControlResult[Option[M1]] =
        new ControlResult((ec: ExecutionContext) =>
          layer
            .withKeyMaterializedValue(k)(identity)
            .flatMap {
              case Some(v) =>
                Future.successful(
                  Some(Try(v.asInstanceOf[M1]).fold(_ => None, Some(_)))
                )
              case None =>
                layer
                  .materializeKey(k)
                  .map(v => Some(Try(v.asInstanceOf[M1]).fold(_ => None, Some(_))))(ec)
            }(ec)
        )

      /** Completes the partition with the specified key.
        *
        * @param k
        *   The partition key to complete.
        * @return
        *   A control result indicating the success of the completion process.
        */
      def completeKey(k: K): ControlResult[Done] =
        new ControlResult(ec => layer.completeKey(k).map(Some(_))(ec))

      /** Request the materialization of the specified partition key and return its materialized
        * value.
        *
        * In case the partition was already materialized, it just returns its materialized value.
        *
        * @param k
        *   The partition key to materialize
        * @return
        *   A control result containing the materialization value of the newly materialized
        *   partition.
        */
      def materializeKey(k: K): ControlResult[M] =
        new ControlResult(ec => layer.materializeKey(k).map(Some(_))(ec))

      /** Request the re-materialization (i.e. completion followed by materialization) of the
        * specified partition key.
        *
        * @param k
        *   The partition key to re-materialize
        * @return
        *   A control result containing the materialization value of the completed partition (if
        *   present) and the materialization value of the newly materialized partition
        */
      def rematerializeKey(k: K): ControlResult[(Option[M], M)] =
        new ControlResult(ec => layer.rematerializeKey(k).map(Some(_))(ec))
    }

    /** A description of the operation to be performed on a [[PartitionControl]] object to obtain a
      * result.
      *
      * Allows for monadic interaction with partition trees and defers the execution of the
      * described operations until explicitly requested via the [[run]] method.
      *
      * {{{
      *   (for {
      *     r1 <- control.materializeKey(1)
      *     r2 <- control.materializeKey(2)
      *   } yield r1 -> r2).run
      * }}}
      */
    final class ControlResult[T] private[spekka] (resultF: ExecutionContext => Future[Option[T]]) {
      def withFilter(f: T => Boolean): ControlResult[T] = {
        val resultF1 = (e: ExecutionContext) =>
          resultF(e).flatMap {
            case Some(result) if f(result) => Future.successful(Some(result))
            case _ => Future.successful(None)
          }(e)

        new ControlResult(resultF1)
      }

      /** Transform the value held by this control result object
        *
        * @param f
        *   transform function
        * @return
        *   A new control result containing the transformed value
        */
      def map[U](f: T => U): ControlResult[U] = {
        val resultF1 = (e: ExecutionContext) => resultF(e).map(_.map(f))(e)
        new ControlResult(resultF1)
      }

      /** Transforms this control result by using its value to build a new control result.
        *
        * @param f
        *   transform function
        * @return
        *   A new control result containing the new control result
        */
      def flatMap[U](f: T => ControlResult[U]): ControlResult[U] = {
        val resultF1 = (e: ExecutionContext) =>
          resultF(e).flatMap {
            case Some(result) => f(result).run(e)
            case None => Future.successful(None)
          }(e)

        new ControlResult(resultF1)
      }

      /** Execute the actions described in this control result and returns the final value of the
        * execution.
        *
        * The same instance can be run multiple time to performs the described actions again.
        *
        * @param ec
        *   The execution context used to chain async operations.
        * @return
        *   The final value of the actions described by this control result.
        */
      def run(implicit ec: ExecutionContext): Future[Option[T]] = {
        resultF(ec)
      }
    }
  }
}

/** Builder for partition trees.
  *
  * {{{
  * case class Input(k1: Int, k2: String, k3: Boolean)
  *
  * Partition.treeBuilder[Input, NotUsed]
  *   .dynamicAuto { case (in, _) => in.k1 }
  *   .dynamicAuto { case (in, _) => in.k2 }
  *   .dynamicAuto { case (in, _) => in.k3 }
  *   .build { case k3 :@: k2 :@: k1 :@: KNil =>
  *     FlowWithExtendedContext[Input, NotUsed].map { _ =>
  *       (k1, k2, k3)
  *     }
  *   }
  * }}}
  *
  * @tparam In
  *   The input type of the partitioned flow to be constructed
  * @tparam Ctx
  *   The base context type of the partitioned flow to be constructed
  */
class PartitionTreeBuilder[In, Ctx] private[spekka] {
  import PartitionTree._

  /** Properties of a partition layer
    */
  sealed trait PartitioningProps[K, PKS <: KSeq] {
    private[PartitionTreeBuilder] type MV[M]
    private[PartitionTreeBuilder] type FOut[O]

    private[spekka] def build[O, M](
        parentKeys: PKS,
        flowF: K => FlowWithExtendedContext[In, O, Ctx, M]
      ): FlowWithExtendedContext[In, FOut[O], Ctx, MV[M]]
  }

  /** Partitioning properties namespace object
    */
  object PartitioningProps {

    /** Simple partitioning where an input corresponds to one output
      */
    sealed trait OneForOne[K, PKS <: KSeq] extends PartitioningProps[K, PKS] {
      override private[PartitionTreeBuilder] type FOut[O] = O
    }

    /** Partitioning where an input may correspond to zero or one output
      */
    sealed trait Optional[K, PKS <: KSeq] extends PartitioningProps[K, PKS] {
      override private[PartitionTreeBuilder] type FOut[O] = Option[O]
    }

    /** [[OneForOne]] adapter for [[Optional]]
      */
    class OneForOneAsOptional[K, PKS <: KSeq, P <: OneForOne[K, PKS]] private[spekka] (oneForOne: P)
        extends Optional[K, PKS] {
      override private[PartitionTreeBuilder] type MV[M] = P#MV[M]
      override def build[O, M](
          parentKeys: PKS,
          flowF: K => FlowWithExtendedContext[In, O, Ctx, M]
        ): FlowWithExtendedContext[In, FOut[O], Ctx, MV[M]] = {
        oneForOne.build[O, M](parentKeys, k => flowF(k)).map(Some(_))
      }
    }

    /** Partitioning where an input correspond to multiple outputs
      */
    sealed trait Multi[K, PKS <: KSeq] extends PartitioningProps[K, PKS] {
      override private[PartitionTreeBuilder] type FOut[O] = immutable.Iterable[O]
    }

    /** [[OneForOne]] adapter for [[Multi]]
      */
    class OneForOneAsMulti[K, PKS <: KSeq, P <: OneForOne[K, PKS]] private[spekka] (oneForOne: P)
        extends Multi[K, PKS] {
      override private[PartitionTreeBuilder] type MV[M] = P#MV[M]
      override def build[O, M](
          parentKeys: PKS,
          flowF: K => FlowWithExtendedContext[In, O, Ctx, M]
        ): FlowWithExtendedContext[In, FOut[O], Ctx, MV[M]] = {
        oneForOne.build[O, M](parentKeys, k => flowF(k)).map(List(_))
      }
    }

    /** [[Optional]] adapter for [[Multi]]
      */
    class OptionalAsMulti[K, PKS <: KSeq, P <: Optional[K, PKS]] private[spekka] (optional: P)
        extends Multi[K, PKS] {
      override private[PartitionTreeBuilder] type MV[M] = P#MV[M]
      override def build[O, M](
          parentKeys: PKS,
          flowF: K => FlowWithExtendedContext[In, O, Ctx, M]
        ): FlowWithExtendedContext[In, FOut[O], Ctx, MV[M]] = {
        optional.build[O, M](parentKeys, k => flowF(k)).map(_.toList)
      }
    }

    /** Dynamic unicast partitioning properties with automatic materialization
      */
    class SingleDynamicAuto[K, PKS <: KSeq] private[spekka] (
        extractor: (In, Ctx, PKS) => K,
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx],
        bufferSize: Int)
        extends OneForOne[K, PKS] {
      override private[PartitionTreeBuilder] type MV[M] = PartitionControl.DynamicControl[K, M]
      override def build[O, M](
          parentKeys: PKS,
          flowF: K => FlowWithExtendedContext[In, O, Ctx, M]
        ): FlowWithExtendedContext[In, FOut[O], Ctx, MV[M]] =
        Partition
          .dynamic(extractor(_, _, parentKeys), flowF, completionCriteria, bufferSize)
          .mapMaterializedValue(new PartitionControl.DynamicControl[K, M](_))
    }

    /** Dynamic unicast partitioning properties with manual materialization
      */
    class SingleDynamicManual[K, PKS <: KSeq] private[spekka] (
        extractor: (In, Ctx, PKS) => K,
        initialKeys: PKS => Set[K],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx],
        bufferSize: Int)
        extends Optional[K, PKS] {
      override private[PartitionTreeBuilder] type MV[M] = PartitionControl.DynamicControl[K, M]
      override def build[O, M](
          parentKeys: PKS,
          flowF: K => FlowWithExtendedContext[In, O, Ctx, M]
        ): FlowWithExtendedContext[In, FOut[O], Ctx, MV[M]] = {
        val optionalFlowF = (k: K) => flowF(k).map(Some(_))
        val passthroughFlow = FlowWithExtendedContext[In, Ctx].map(_ => None)
        Partition
          .dynamicManual(
            extractor(_, _, parentKeys),
            optionalFlowF,
            passthroughFlow,
            completionCriteria,
            initialKeys(parentKeys),
            bufferSize
          )
          .mapMaterializedValue(new PartitionControl.DynamicControl[K, M](_))
      }
    }

    /** Dynamic multicast partitioning properties with automatic materialization
      */
    class MultiDynamicAuto[K, PKS <: KSeq] private[spekka] (
        extractor: (In, Ctx, Set[K], PKS) => Set[K],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx],
        bufferSize: Int)
        extends Multi[K, PKS] {
      override private[PartitionTreeBuilder] type MV[M] = PartitionControl.DynamicControl[K, M]
      override def build[O, M](
          parentKeys: PKS,
          flowF: K => FlowWithExtendedContext[In, O, Ctx, M]
        ): FlowWithExtendedContext[In, FOut[O], Ctx, MV[M]] = {
        val optionalFlowF = (k: K) => flowF(k).map(Some(_))
        val passthroughFlow = FlowWithExtendedContext[In, Ctx].map(_ => None)
        Partition
          .dynamicMulti(
            extractor(_, _, _, parentKeys),
            optionalFlowF,
            passthroughFlow,
            completionCriteria,
            bufferSize
          )
          .map(_.flatten)
          .mapMaterializedValue(new PartitionControl.DynamicControl[K, M](_))
      }
    }

    /** Dynamic multicast partitioning properties with manual materialization
      */
    class MultiDynamicManual[K, PKS <: KSeq] private[spekka] (
        extractor: (In, Ctx, Set[K], PKS) => Set[K],
        initialKeys: PKS => Set[K],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx],
        bufferSize: Int)
        extends Multi[K, PKS] {
      override private[PartitionTreeBuilder] type MV[M] = PartitionControl.DynamicControl[K, M]
      override def build[O, M](
          parentKeys: PKS,
          flowF: K => FlowWithExtendedContext[In, O, Ctx, M]
        ): FlowWithExtendedContext[In, FOut[O], Ctx, MV[M]] = {
        val optionalFlowF = (k: K) => flowF(k).map(Some(_))
        val passthroughFlow = FlowWithExtendedContext[In, Ctx].map(_ => None)
        Partition
          .dynamicMultiManual(
            extractor(_, _, _, parentKeys),
            optionalFlowF,
            passthroughFlow,
            completionCriteria,
            initialKeys(parentKeys),
            bufferSize
          )
          .map(_.flatten)
          .mapMaterializedValue(new PartitionControl.DynamicControl[K, M](_))
      }
    }

    /** Static unicast partitioning properties
      */
    class SingleStatic[K, PKS <: KSeq] private[spekka] (
        extractor: (In, Ctx, PKS) => K,
        keys: PKS => Set[K])
        extends OneForOne[K, PKS] {
      override private[PartitionTreeBuilder] type MV[M] = PartitionControl.StaticControl[K, M]
      override def build[O, M](
          parentKeys: PKS,
          flowF: K => FlowWithExtendedContext[In, O, Ctx, M]
        ): FlowWithExtendedContext[In, FOut[O], Ctx, MV[M]] = {
        val passthroughFlow = FlowWithExtendedContext[In, Ctx].map(in =>
          throw new IllegalArgumentException(s"No flow defined SingleStatic router input: ${in}")
        )
        val flowMap = keys(parentKeys).iterator.map(k => k -> flowF(k)).toMap
        Partition
          .static(extractor(_, _, parentKeys), flowMap, passthroughFlow)
          .mapMaterializedValue(new PartitionControl.StaticControl[K, M](_))
      }
    }

    /** Static multicast partitioning properties
      */
    class MultiStatic[K, PKS <: KSeq] private[spekka] (
        extractor: (In, Ctx, Set[K], PKS) => Set[K],
        keys: PKS => Set[K])
        extends Multi[K, PKS] {
      override private[PartitionTreeBuilder] type MV[M] = PartitionControl.StaticControl[K, M]
      override def build[O, M](
          parentKeys: PKS,
          flowF: K => FlowWithExtendedContext[In, O, Ctx, M]
        ): FlowWithExtendedContext[In, FOut[O], Ctx, MV[M]] = {
        val passthroughFlow = FlowWithExtendedContext[In, Ctx].map(_ => None)
        val flowMap = keys(parentKeys).iterator.map(k => k -> flowF(k).map(Some(_))).toMap
        Partition
          .staticMulti(extractor(_, _, _, parentKeys), flowMap, passthroughFlow)
          .map(_.flatten)
          .mapMaterializedValue(new PartitionControl.StaticControl[K, M](_))
      }
    }
  }

  /** Partition layer
    */
  sealed trait Layer[MV[_]] {
    private[PartitionTreeBuilder] type FOut[O]
    private[PartitionTreeBuilder] type KS <: KSeq
  }

  object Layer {
    private[PartitionTreeBuilder] trait CanBuildOneForOneT[MV[_], L <: Layer[MV]] {
      def buildOneForOne[O, M](
          layer: L,
          flowF: L#KS => FlowWithExtendedContext[In, O, Ctx, M]
        ): FlowWithExtendedContext[In, O, Ctx, MV[M]]
    }

    private[PartitionTreeBuilder] trait CanBuildOptionalT[MV[_], L <: Layer[MV]] {
      def buildOptional[O, M](
          layer: L,
          flowF: L#KS => FlowWithExtendedContext[In, Option[O], Ctx, M]
        ): FlowWithExtendedContext[In, Option[O], Ctx, MV[M]]
    }

    private[PartitionTreeBuilder] trait CanBuildMultiT[MV[_], L <: Layer[MV]] {
      def buildMulti[O, M](
          layer: L,
          flowF: L#KS => FlowWithExtendedContext[In, immutable.Iterable[O], Ctx, M]
        ): FlowWithExtendedContext[In, immutable.Iterable[O], Ctx, MV[M]]
    }
  }

  case object Root extends Root
  implicit private[PartitionTreeBuilder] val rootCanBuildOneForOneT
      : Layer.CanBuildOneForOneT[Lambda[M => M], Root] =
    new Layer.CanBuildOneForOneT[Lambda[M => M], Root] {
      def buildOneForOne[O, M](
          layer: Root,
          flowF: Root#KS => FlowWithExtendedContext[In, O, Ctx, M]
        ): FlowWithExtendedContext[In, O, Ctx, M] = flowF(KNil)
    }
  implicit private[PartitionTreeBuilder] val rootCanBuildOptionalT
      : Layer.CanBuildOptionalT[Lambda[M => M], Root] =
    new Layer.CanBuildOptionalT[Lambda[M => M], Root] {
      def buildOptional[O, M](
          layer: Root,
          flowF: Root#KS => FlowWithExtendedContext[In, Option[O], Ctx, M]
        ): FlowWithExtendedContext[In, Option[O], Ctx, M] = flowF(KNil)
    }
  implicit private[PartitionTreeBuilder] val rootCanBuildMultiT
      : Layer.CanBuildMultiT[Lambda[M => M], Root] =
    new Layer.CanBuildMultiT[Lambda[M => M], Root] {
      def buildMulti[O, M](
          layer: Root,
          flowF: Root#KS => FlowWithExtendedContext[In, immutable.Iterable[O], Ctx, M]
        ): FlowWithExtendedContext[In, immutable.Iterable[O], Ctx, M] = flowF(KNil)
    }

  /** Root partition layer
    */
  sealed trait Root extends Layer[Lambda[M => M]] {
    override private[PartitionTreeBuilder] type FOut[O] = O
    override private[PartitionTreeBuilder] type KS = KNil

    /** Creates a dynamic unicast partition layer with automatic materialization.
      *
      * Each input will be routed to exactly one partition which will be automatically materialized
      * on the first element.
      *
      * @param extractor
      *   partition key extractor function mapping each (input, context) to exactly one partition
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicAutoCtx[K](
        extractor: (In, Ctx) => K,
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): OneForOne[K, PartitioningProps.SingleDynamicAuto[K, KNil], Lambda[M => M], Root] = {
      val extractorWrapper: (In, Ctx, KNil) => K = (in, ctx, _) => extractor(in, ctx)
      val prop =
        new PartitioningProps.SingleDynamicAuto(extractorWrapper, completionCriteria, bufferSize)
      new OneForOne[K, PartitioningProps.SingleDynamicAuto[K, KNil], Lambda[M => M], Root](
        prop,
        Root
      )
    }

    /** Creates a dynamic unicast partition layer with automatic materialization.
      *
      * Each input will be routed to exactly one partition which will be automatically materialized
      * on the first element.
      *
      * @param extractor
      *   partition key extractor function mapping each input to exactly one partition
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicAuto[K](
        extractor: In => K,
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): OneForOne[K, PartitioningProps.SingleDynamicAuto[K, KNil], Lambda[M => M], Root] = {
      dynamicAutoCtx[K](
        (in: In, _: Ctx) => extractor(in),
        completionCriteria,
        bufferSize
      )
    }

    /** Creates a dynamic multicast partition layer with automatic materialization.
      *
      * Each input will be routed to a variable number of partitions which will be automatically
      * materialized on the first element.
      *
      * Input mapped to an empty set of partition keys are ignored, but their context is preserved.
      *
      * The third parameter of the extractor function are the currently materialized partition.
      *
      * @param extractor
      *   partition key extractor function mapping each (input, context) to a set (potentially
      *   empty) of partitions
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicAutoMulticastCtx[K](
        extractor: (In, Ctx, Set[K]) => Set[K],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Multi[K, PartitioningProps.MultiDynamicAuto[K, KNil], Lambda[M => M], Root] = {
      val extractorWrapper: (In, Ctx, Set[K], KNil) => Set[K] = (in, ctx, ks, _) =>
        extractor(in, ctx, ks)
      val prop =
        new PartitioningProps.MultiDynamicAuto(extractorWrapper, completionCriteria, bufferSize)
      new Multi[K, PartitioningProps.MultiDynamicAuto[K, KNil], Lambda[M => M], Root](prop, Root)
    }

    /** Creates a dynamic multicast partition layer with automatic materialization.
      *
      * Each input will be routed to a variable number of partitions which will be automatically
      * materialized on the first element.
      *
      * Input mapped to an empty set of partition keys are ignored, but their context is preserved.
      *
      * The second parameter of the extractor function are the currently materialized partition.
      *
      * @param extractor
      *   partition key extractor function mapping each input to a set (potentially empty) of
      *   partitions
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicAutoMulticast[K](
        extractor: (In, Set[K]) => Set[K],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Multi[K, PartitioningProps.MultiDynamicAuto[K, KNil], Lambda[M => M], Root] = {
      dynamicAutoMulticastCtx(
        (in: In, _: Ctx, keys: Set[K]) => extractor(in, keys),
        completionCriteria,
        bufferSize
      )
    }

    /** Creates a dynamic unicast partition layer with manual materialization.
      *
      * Each input will be routed to exactly one partition which must be materialized manually.
      *
      * Input destined to non materialized partition keys are ignored, but their context is
      * preserved.
      *
      * @param extractor
      *   partition key extractor function mapping each (input, context) to exactly one partition
      * @param initialKeys
      *   the set of keys to materialized when the layer is initialized
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicManualCtx[K](
        extractor: (In, Ctx) => K,
        initialKeys: Set[K],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Optional[K, PartitioningProps.SingleDynamicManual[K, KNil], Lambda[M => M], Root] = {
      val extractorWrapper: (In, Ctx, KNil) => K = (in, ctx, _) => extractor(in, ctx)
      val initialKeysWrapper: KNil => Set[K] = _ => initialKeys
      val prop =
        new PartitioningProps.SingleDynamicManual(
          extractorWrapper,
          initialKeysWrapper,
          completionCriteria,
          bufferSize
        )
      new Optional[K, PartitioningProps.SingleDynamicManual[K, KNil], Lambda[M => M], Root](
        prop,
        Root
      )
    }

    /** Creates a dynamic unicast partition layer with manual materialization.
      *
      * Each input will be routed to exactly one partition which must be materialized manually.
      *
      * Input destined to non materialized partition keys are ignored, but their context is
      * preserved.
      *
      * @param extractor
      *   partition key extractor function mapping each input to exactly one partition
      * @param initialKeys
      *   the set of keys to materialized when the layer is initialized
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicManual[K](
        extractor: In => K,
        initialKeys: Set[K],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Optional[K, PartitioningProps.SingleDynamicManual[K, KNil], Lambda[M => M], Root] = {
      dynamicManualCtx(
        (in: In, _: Ctx) => extractor(in),
        initialKeys,
        completionCriteria,
        bufferSize
      )
    }

    /** Creates a dynamic multicast partition layer with manual materialization.
      *
      * Each input will be routed to a variable number of partitions which must be materialized
      * manually.
      *
      * Input destined to non materialized partition keys are ignored, but their context is
      * preserved.
      *
      * The third parameter of the extractor function are the currently materialized partition.
      *
      * @param extractor
      *   partition key extractor function mapping each (input, context) to a set (potentially
      *   empty) of partitions
      * @param initialKeys
      *   the set of keys to materialized when the layer is initialized
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicManualMulticastCtx[K](
        extractor: (In, Ctx, Set[K]) => Set[K],
        initialKeys: Set[K],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Multi[K, PartitioningProps.MultiDynamicManual[K, KNil], Lambda[M => M], Root] = {
      val extractorWrapper: (In, Ctx, Set[K], KNil) => Set[K] = (in, ctx, ks, _) =>
        extractor(in, ctx, ks)
      val initialKeysWrapper: KNil => Set[K] = _ => initialKeys
      val prop =
        new PartitioningProps.MultiDynamicManual(
          extractorWrapper,
          initialKeysWrapper,
          completionCriteria,
          bufferSize
        )
      new Multi[K, PartitioningProps.MultiDynamicManual[K, KNil], Lambda[M => M], Root](prop, Root)
    }

    /** Creates a dynamic multicast partition layer with manual materialization.
      *
      * Each input will be routed to a variable number of partitions which must be materialized
      * manually.
      *
      * Input destined to non materialized partition keys are ignored, but their context is
      * preserved.
      *
      * The second parameter of the extractor function are the currently materialized partition.
      *
      * @param extractor
      *   partition key extractor function mapping each input to a set (potentially empty) of
      *   partitions
      * @param initialKeys
      *   the set of keys to materialized when the layer is initialized
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicManualMulticast[K](
        extractor: (In, Set[K]) => Set[K],
        initialKeys: Set[K],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Multi[K, PartitioningProps.MultiDynamicManual[K, KNil], Lambda[M => M], Root] = {
      dynamicManualMulticastCtx(
        (in: In, _: Ctx, keys: Set[K]) => extractor(in, keys),
        initialKeys,
        completionCriteria,
        bufferSize
      )
    }

    /** Creates a static unicast partition layer.
      *
      * Each input will be routed to exactly one partition.
      *
      * In case an input is mapped to a non existing partition key the stream will fail.
      *
      * @param extractor
      *   partition key extractor function mapping each (input, context) to exactly one partition
      * @param keys
      *   the partition keys to materialize. MUST contain all the possible outputs of `extractor`
      * @return
      *   partition builder
      */
    def staticCtx[K](
        extractor: (In, Ctx) => K,
        keys: Set[K]
      ): OneForOne[K, PartitioningProps.SingleStatic[K, KNil], Lambda[M => M], Root] = {
      val extractorWrapper: (In, Ctx, KNil) => K = (in, ctx, _) => extractor(in, ctx)
      val keysWrapper: KS => Set[K] = _ => keys
      val prop = new PartitioningProps.SingleStatic(extractorWrapper, keysWrapper)
      new OneForOne[K, PartitioningProps.SingleStatic[K, KNil], Lambda[M => M], Root](prop, Root)
    }

    /** Creates a static unicast partition layer.
      *
      * Each input will be routed to exactly one partition.
      *
      * In case an input is mapped to a non existing partition key the stream will fail.
      *
      * @param extractor
      *   partition key extractor function mapping each input to exactly one partition
      * @param keys
      *   the partition keys to materialize. MUST contain all the possible outputs of `extractor`
      * @return
      *   partition builder
      */
    def static[K](
        extractor: In => K,
        keys: Set[K]
      ): OneForOne[K, PartitioningProps.SingleStatic[K, KNil], Lambda[M => M], Root] = {
      staticCtx(
        (in: In, _: Ctx) => extractor(in),
        keys
      )
    }

    /** Creates a static multicast partition layer.
      *
      * Each input will be routed to a variable number of partitions.
      *
      * In case an input is mapped to a non existing partition key the stream will fail.
      *
      * The third parameter of the extractor function are the currently materialized partition.
      *
      * @param extractor
      *   partition key extractor function mapping each (input, context) to a set (potentially
      *   empty) of partition
      * @param keys
      *   the partition keys to materialize. MUST contain all the possible outputs of `extractor`
      * @return
      *   partition builder
      */
    def staticMulticastCtx[K](
        extractor: (In, Ctx, Set[K]) => Set[K],
        keys: Set[K]
      ): Multi[K, PartitioningProps.MultiStatic[K, KNil], Lambda[M => M], Root] = {
      val extractorWrapper: (In, Ctx, Set[K], KNil) => Set[K] = (in, ctx, ks, _) =>
        extractor(in, ctx, ks)
      val keysWrapper: KNil => Set[K] = _ => keys
      val prop = new PartitioningProps.MultiStatic(extractorWrapper, keysWrapper)
      new Multi[K, PartitioningProps.MultiStatic[K, KNil], Lambda[M => M], Root](prop, Root)
    }

    /** Creates a static multicast partition layer.
      *
      * Each input will be routed to a variable number of partitions.
      *
      * In case an input is mapped to a non existing partition key the stream will fail.
      *
      * The second parameter of the extractor function are the currently materialized partition.
      *
      * @param extractor
      *   partition key extractor function mapping each input to a set (potentially empty) of
      *   partition
      * @param keys
      *   the partition keys to materialize. MUST contain all the possible outputs of `extractor`
      * @return
      *   partition builder
      */
    def staticMulticast[K](
        extractor: (In, Set[K]) => Set[K],
        keys: Set[K]
      ): Multi[K, PartitioningProps.MultiStatic[K, KNil], Lambda[M => M], Root] = {
      staticMulticastCtx(
        (in: In, _: Ctx, keys: Set[K]) => extractor(in, keys),
        keys
      )
    }
  }

  implicit private[PartitionTreeBuilder] def oneForOneCanBuildOneForOneT[
      K,
      Props <: PartitioningProps.OneForOne[K, Parent#KS],
      ParentMV[_],
      Parent <: Layer[ParentMV],
      L <: OneForOne[K, Props, ParentMV, Parent]
    ](implicit ev: Layer.CanBuildOneForOneT[ParentMV, Parent]
    ) =
    new Layer.CanBuildOneForOneT[Lambda[M => ParentMV[Props#MV[M]]], L] {
      def buildOneForOne[O, M](
          layer: L,
          flowF: L#KS => FlowWithExtendedContext[In, O, Ctx, M]
        ): FlowWithExtendedContext[In, O, Ctx, ParentMV[Props#MV[M]]] = {
        val levelFlowF = (ks: Parent#KS) => layer.props.build[O, M](ks, k => flowF(k :@: ks))
        ev.buildOneForOne(layer.parent, levelFlowF)
      }
    }

  implicit private[PartitionTreeBuilder] def oneForOneCanBuildOptionalT[
      K,
      Props <: PartitioningProps.OneForOne[K, Parent#KS],
      ParentMV[_],
      Parent <: Layer[ParentMV],
      L <: OneForOne[K, Props, ParentMV, Parent]
    ](implicit ev: Layer.CanBuildOneForOneT[ParentMV, Parent]
    ) =
    new Layer.CanBuildOptionalT[Lambda[M => ParentMV[Props#MV[M]]], L] {
      def buildOptional[O, M](
          layer: L,
          flowF: L#KS => FlowWithExtendedContext[In, Option[O], Ctx, M]
        ): FlowWithExtendedContext[In, Option[O], Ctx, ParentMV[Props#MV[M]]] = {
        val levelFlowF = (ks: Parent#KS) =>
          layer.props.build[Option[O], M](ks, k => flowF(k :@: ks))
        ev.buildOneForOne(layer.parent, levelFlowF)
      }
    }

  implicit private[PartitionTreeBuilder] def oneForOneCanBuildMultiT[
      K,
      Props <: PartitioningProps.OneForOne[K, Parent#KS],
      ParentMV[_],
      Parent <: Layer[ParentMV],
      L <: OneForOne[K, Props, ParentMV, Parent]
    ](implicit ev: Layer.CanBuildOneForOneT[ParentMV, Parent]
    ) =
    new Layer.CanBuildMultiT[Lambda[M => ParentMV[Props#MV[M]]], L] {
      def buildMulti[O, M](
          layer: L,
          flowF: L#KS => FlowWithExtendedContext[In, immutable.Iterable[O], Ctx, M]
        ): FlowWithExtendedContext[In, immutable.Iterable[O], Ctx, ParentMV[Props#MV[M]]] = {
        val levelFlowF = (ks: Parent#KS) =>
          layer.props.build[immutable.Iterable[O], M](ks, k => flowF(k :@: ks))
        ev.buildOneForOne(layer.parent, levelFlowF)
      }
    }

  /** A one for one partition layer.
    *
    * Given one input the layer will produce exactly one output.
    */
  class OneForOne[
      K,
      Props <: PartitioningProps.OneForOne[K, Parent#KS],
      ParentMV[_],
      Parent <: Layer[ParentMV]
    ] private[spekka] (
      private[spekka] val props: Props,
      private[spekka] val parent: Parent
    )(implicit ev: Layer.CanBuildOneForOneT[ParentMV, Parent])
      extends Layer[Lambda[M => ParentMV[Props#MV[M]]]] {
    override private[PartitionTreeBuilder] type KS = K :@: Parent#KS

    implicit private val canBuildOneForOne
        : Layer.CanBuildOneForOneT[Lambda[M => ParentMV[Props#MV[M]]], OneForOne[K, Props, ParentMV, Parent]] =
      oneForOneCanBuildOneForOneT[K, Props, ParentMV, Parent, OneForOne[K, Props, ParentMV, Parent]]
    implicit private val canBuildOptional
        : Layer.CanBuildOptionalT[Lambda[M => ParentMV[Props#MV[M]]], OneForOne[K, Props, ParentMV, Parent]] =
      oneForOneCanBuildOptionalT[K, Props, ParentMV, Parent, OneForOne[K, Props, ParentMV, Parent]]
    implicit private val canBuildMulti
        : Layer.CanBuildMultiT[Lambda[M => ParentMV[Props#MV[M]]], OneForOne[K, Props, ParentMV, Parent]] =
      oneForOneCanBuildMultiT[K, Props, ParentMV, Parent, OneForOne[K, Props, ParentMV, Parent]]

    def build[O, M](
        flowF: KS => FlowWithExtendedContext[In, O, Ctx, M]
      ): FlowWithExtendedContext[In, O, Ctx, ParentMV[Props#MV[M]]] = {
      canBuildOneForOne.buildOneForOne(
        this,
        flowF
      )
    }

    /** Creates a dynamic unicast partition layer with automatic materialization.
      *
      * Each input will be routed to exactly one partition which will be automatically materialized
      * on the first element.
      *
      * @param extractor
      *   partition key extractor function mapping each (input, context, parent keys) to exactly one
      *   partition
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicAutoCtxWithKeys[K1](
        extractor: (In, Ctx, KS) => K1,
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): OneForOne[K1, PartitioningProps.SingleDynamicAuto[K1, KS], Lambda[M => ParentMV[Props#MV[M]]], OneForOne[K, Props, ParentMV, Parent]] = {
      val prop: PartitioningProps.SingleDynamicAuto[K1, KS] =
        new PartitioningProps.SingleDynamicAuto(extractor, completionCriteria, bufferSize)
      new OneForOne[K1, PartitioningProps.SingleDynamicAuto[K1, KS], Lambda[
        M => ParentMV[Props#MV[M]]
      ], OneForOne[K, Props, ParentMV, Parent]](prop, this)
    }

    /** Creates a dynamic unicast partition layer with automatic materialization.
      *
      * Each input will be routed to exactly one partition which will be automatically materialized
      * on the first element.
      *
      * @param extractor
      *   partition key extractor function mapping each (input, context) to exactly one partition
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicAutoCtx[K1](
        extractor: (In, Ctx) => K1,
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): OneForOne[K1, PartitioningProps.SingleDynamicAuto[K1, KS], Lambda[M => ParentMV[Props#MV[M]]], OneForOne[K, Props, ParentMV, Parent]] = {
      val extractorWrapper: (In, Ctx, KS) => K1 = (in, ctx, _) => extractor(in, ctx)
      dynamicAutoCtxWithKeys(extractorWrapper, completionCriteria, bufferSize)
    }

    /** Creates a dynamic unicast partition layer with automatic materialization.
      *
      * Each input will be routed to exactly one partition which will be automatically materialized
      * on the first element.
      *
      * @param extractor
      *   partition key extractor function mapping each input to exactly one partition
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicAuto[K1](
        extractor: In => K1,
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): OneForOne[K1, PartitioningProps.SingleDynamicAuto[K1, KS], Lambda[M => ParentMV[Props#MV[M]]], OneForOne[K, Props, ParentMV, Parent]] = {
      dynamicAutoCtx(
        (in: In, _: Ctx) => extractor(in),
        completionCriteria,
        bufferSize
      )
    }

    /** Creates a dynamic multicast partition layer with automatic materialization.
      *
      * Each input will be routed to a variable number of partitions which will be automatically
      * materialized on the first element.
      *
      * Input mapped to an empty set of partition keys are ignored, but their context is preserved.
      *
      * The third parameter of the extractor function are the currently materialized partition.
      *
      * @param extractor
      *   partition key extractor function mapping each (input, context, parent keys) to a set
      *   (potentially empty) of partitions
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicAutoMulticastCtxWithKeys[K1](
        extractor: (In, Ctx, Set[K1], KS) => Set[K1],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Multi[K1, PartitioningProps.MultiDynamicAuto[K1, KS], Lambda[M => ParentMV[Props#MV[M]]], OneForOne[K, Props, ParentMV, Parent]] = {
      val prop =
        new PartitioningProps.MultiDynamicAuto[K1, KS](extractor, completionCriteria, bufferSize)
      new Multi[K1, PartitioningProps.MultiDynamicAuto[K1, KS], Lambda[
        M => ParentMV[Props#MV[M]]
      ], OneForOne[K, Props, ParentMV, Parent]](prop, this)
    }

    /** Creates a dynamic multicast partition layer with automatic materialization.
      *
      * Each input will be routed to a variable number of partitions which will be automatically
      * materialized on the first element.
      *
      * Input mapped to an empty set of partition keys are ignored, but their context is preserved.
      *
      * The third parameter of the extractor function are the currently materialized partition.
      *
      * @param extractor
      *   partition key extractor function mapping each (input, context) to a set (potentially
      *   empty) of partitions
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicAutoMulticastCtx[K1](
        extractor: (In, Ctx, Set[K1]) => Set[K1],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Multi[K1, PartitioningProps.MultiDynamicAuto[K1, KS], Lambda[M => ParentMV[Props#MV[M]]], OneForOne[K, Props, ParentMV, Parent]] = {
      val extractorWrapper: (In, Ctx, Set[K1], KS) => Set[K1] = (in, ctx, ks, _) =>
        extractor(in, ctx, ks)
      dynamicAutoMulticastCtxWithKeys(extractorWrapper, completionCriteria, bufferSize)
    }

    /** Creates a dynamic multicast partition layer with automatic materialization.
      *
      * Each input will be routed to a variable number of partitions which will be automatically
      * materialized on the first element.
      *
      * Input mapped to an empty set of partition keys are ignored, but their context is preserved.
      *
      * The second parameter of the extractor function are the currently materialized partition.
      *
      * @param extractor
      *   partition key extractor function mapping each input to a set (potentially empty) of
      *   partitions
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicAutoMulticast[K1](
        extractor: (In, Set[K1]) => Set[K1],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Multi[K1, PartitioningProps.MultiDynamicAuto[K1, KS], Lambda[M => ParentMV[Props#MV[M]]], OneForOne[K, Props, ParentMV, Parent]] = {
      dynamicAutoMulticastCtx(
        (in: In, _: Ctx, keys: Set[K1]) => extractor(in, keys),
        completionCriteria,
        bufferSize
      )
    }

    /** Creates a dynamic unicast partition layer with manual materialization.
      *
      * Each input will be routed to exactly one partition which must be materialized manually.
      *
      * Input destined to non materialized partition keys are ignored, but their context is
      * preserved.
      *
      * @param extractor
      *   partition key extractor function mapping each (input, context, parent keys) to exactly one
      *   partition
      * @param initialKeys
      *   the set of keys to materialized when the layer is initialized
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicManualCtxWithKeys[K1](
        extractor: (In, Ctx, KS) => K1,
        initialKeys: KS => Set[K1],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Optional[K1, PartitioningProps.SingleDynamicManual[K1, KS], Lambda[M => ParentMV[Props#MV[M]]], OneForOne[K, Props, ParentMV, Parent]] = {
      val prop =
        new PartitioningProps.SingleDynamicManual(
          extractor,
          initialKeys,
          completionCriteria,
          bufferSize
        )
      new Optional[K1, PartitioningProps.SingleDynamicManual[K1, KS], Lambda[
        M => ParentMV[Props#MV[M]]
      ], OneForOne[K, Props, ParentMV, Parent]](prop, this)
    }

    /** Creates a dynamic unicast partition layer with manual materialization.
      *
      * Each input will be routed to exactly one partition which must be materialized manually.
      *
      * Input destined to non materialized partition keys are ignored, but their context is
      * preserved.
      *
      * @param extractor
      *   partition key extractor function mapping each (input, context) to exactly one partition
      * @param initialKeys
      *   the set of keys to materialized when the layer is initialized
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicManualCtx[K1](
        extractor: (In, Ctx) => K1,
        initialKeys: Set[K1],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Optional[K1, PartitioningProps.SingleDynamicManual[K1, KS], Lambda[M => ParentMV[Props#MV[M]]], OneForOne[K, Props, ParentMV, Parent]] = {
      val extractorWrapper: (In, Ctx, KS) => K1 = (in, ctx, _) => extractor(in, ctx)
      val initialKeysWrapper: KS => Set[K1] = _ => initialKeys
      dynamicManualCtxWithKeys(extractorWrapper, initialKeysWrapper, completionCriteria, bufferSize)
    }

    /** Creates a dynamic unicast partition layer with manual materialization.
      *
      * Each input will be routed to exactly one partition which must be materialized manually.
      *
      * Input destined to non materialized partition keys are ignored, but their context is
      * preserved.
      *
      * @param extractor
      *   partition key extractor function mapping each input to exactly one partition
      * @param initialKeys
      *   the set of keys to materialized when the layer is initialized
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicManual[K1](
        extractor: In => K1,
        initialKeys: Set[K1],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Optional[K1, PartitioningProps.SingleDynamicManual[K1, KS], Lambda[M => ParentMV[Props#MV[M]]], OneForOne[K, Props, ParentMV, Parent]] = {
      dynamicManualCtx(
        (in: In, _: Ctx) => extractor(in),
        initialKeys,
        completionCriteria,
        bufferSize
      )
    }

    /** Creates a dynamic multicast partition layer with manual materialization.
      *
      * Each input will be routed to a variable number of partitions which must be materialized
      * manually.
      *
      * Input destined to non materialized partition keys are ignored, but their context is
      * preserved.
      *
      * The third parameter of the extractor function are the currently materialized partition.
      *
      * @param extractor
      *   partition key extractor function mapping each (input, context, parent keys) to a set
      *   (potentially empty) of partitions
      * @param initialKeys
      *   the set of keys to materialized when the layer is initialized
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicManualMulticastCtxWithKeys[K1](
        extractor: (In, Ctx, Set[K1], KS) => Set[K1],
        initialKeys: KS => Set[K1],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Multi[K1, PartitioningProps.MultiDynamicManual[K1, KS], Lambda[M => ParentMV[Props#MV[M]]], OneForOne[K, Props, ParentMV, Parent]] = {
      val prop =
        new PartitioningProps.MultiDynamicManual(
          extractor,
          initialKeys,
          completionCriteria,
          bufferSize
        )
      new Multi[K1, PartitioningProps.MultiDynamicManual[K1, KS], Lambda[
        M => ParentMV[Props#MV[M]]
      ], OneForOne[K, Props, ParentMV, Parent]](prop, this)
    }

    /** Creates a dynamic multicast partition layer with manual materialization.
      *
      * Each input will be routed to a variable number of partitions which must be materialized
      * manually.
      *
      * Input destined to non materialized partition keys are ignored, but their context is
      * preserved.
      *
      * The third parameter of the extractor function are the currently materialized partition.
      *
      * @param extractor
      *   partition key extractor function mapping each (input, context) to a set (potentially
      *   empty) of partitions
      * @param initialKeys
      *   the set of keys to materialized when the layer is initialized
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicManualMulticastCtx[K1](
        extractor: (In, Ctx, Set[K1]) => Set[K1],
        initialKeys: Set[K1],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Multi[K1, PartitioningProps.MultiDynamicManual[K1, KS], Lambda[M => ParentMV[Props#MV[M]]], OneForOne[K, Props, ParentMV, Parent]] = {
      val extractorWrapper: (In, Ctx, Set[K1], KS) => Set[K1] = (in, ctx, ks, _) =>
        extractor(in, ctx, ks)
      val initialKeysWrapper: KS => Set[K1] = _ => initialKeys
      dynamicManualMulticastCtxWithKeys(
        extractorWrapper,
        initialKeysWrapper,
        completionCriteria,
        bufferSize
      )
    }

    /** Creates a dynamic multicast partition layer with manual materialization.
      *
      * Each input will be routed to a variable number of partitions which must be materialized
      * manually.
      *
      * Input destined to non materialized partition keys are ignored, but their context is
      * preserved.
      *
      * The second parameter of the extractor function are the currently materialized partition.
      *
      * @param extractor
      *   partition key extractor function mapping each input to a set (potentially empty) of
      *   partitions
      * @param initialKeys
      *   the set of keys to materialized when the layer is initialized
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicManualMulticast[K1](
        extractor: (In, Set[K1]) => Set[K1],
        initialKeys: Set[K1],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Multi[K1, PartitioningProps.MultiDynamicManual[K1, KS], Lambda[M => ParentMV[Props#MV[M]]], OneForOne[K, Props, ParentMV, Parent]] = {
      dynamicManualMulticastCtx(
        (in: In, _: Ctx, keys: Set[K1]) => extractor(in, keys),
        initialKeys,
        completionCriteria,
        bufferSize
      )
    }

    /** Creates a static unicast partition layer.
      *
      * Each input will be routed to exactly one partition.
      *
      * In case an input is mapped to a non existing partition key the stream will fail.
      *
      * @param extractor
      *   partition key extractor function mapping each (input, context, parent keys) to exactly one
      *   partition
      * @param keys
      *   the partition keys to materialize. MUST contain all the possible outputs of `extractor`
      * @return
      *   partition builder
      */
    def staticCtxWithKeys[K1](
        extractor: (In, Ctx, KS) => K1,
        keys: KS => Set[K1]
      ): OneForOne[K1, PartitioningProps.SingleStatic[K1, KS], Lambda[M => ParentMV[Props#MV[M]]], OneForOne[K, Props, ParentMV, Parent]] = {
      val prop = new PartitioningProps.SingleStatic(extractor, keys)
      new OneForOne[K1, PartitioningProps.SingleStatic[K1, KS], Lambda[
        M => ParentMV[Props#MV[M]]
      ], OneForOne[K, Props, ParentMV, Parent]](prop, this)
    }

    /** Creates a static unicast partition layer.
      *
      * Each input will be routed to exactly one partition.
      *
      * In case an input is mapped to a non existing partition key the stream will fail.
      *
      * @param extractor
      *   partition key extractor function mapping each (input, context) to exactly one partition
      * @param keys
      *   the partition keys to materialize. MUST contain all the possible outputs of `extractor`
      * @return
      *   partition builder
      */
    def staticCtx[K1](
        extractor: (In, Ctx) => K1,
        keys: Set[K1]
      ): OneForOne[K1, PartitioningProps.SingleStatic[K1, KS], Lambda[M => ParentMV[Props#MV[M]]], OneForOne[K, Props, ParentMV, Parent]] = {
      val extractorWrapper: (In, Ctx, KS) => K1 = (in, ctx, _) => extractor(in, ctx)
      val keysWrapper: KS => Set[K1] = _ => keys
      staticCtxWithKeys(extractorWrapper, keysWrapper)
    }

    /** Creates a static unicast partition layer.
      *
      * Each input will be routed to exactly one partition.
      *
      * In case an input is mapped to a non existing partition key the stream will fail.
      *
      * @param extractor
      *   partition key extractor function mapping each input to exactly one partition
      * @param keys
      *   the partition keys to materialize. MUST contain all the possible outputs of `extractor`
      * @return
      *   partition builder
      */
    def static[K1](
        extractor: In => K1,
        keys: Set[K1]
      ): OneForOne[K1, PartitioningProps.SingleStatic[K1, KS], Lambda[M => ParentMV[Props#MV[M]]], OneForOne[K, Props, ParentMV, Parent]] = {
      staticCtx(
        (in: In, _: Ctx) => extractor(in),
        keys
      )
    }

    /** Creates a static multicast partition layer.
      *
      * Each input will be routed to a variable number of partitions.
      *
      * In case an input is mapped to a non existing partition key the stream will fail.
      *
      * The third parameter of the extractor function are the currently materialized partition.
      *
      * @param extractor
      *   partition key extractor function mapping each (input, context, parent keys) to a set
      *   (potentially empty) of partition
      * @param keys
      *   the partition keys to materialize. MUST contain all the possible outputs of `extractor`
      * @return
      *   partition builder
      */
    def staticMulticastCtxWithKeys[K1](
        extractor: (In, Ctx, Set[K1], KS) => Set[K1],
        keys: KS => Set[K1]
      ): Multi[K1, PartitioningProps.MultiStatic[K1, KS], Lambda[M => ParentMV[Props#MV[M]]], OneForOne[K, Props, ParentMV, Parent]] = {
      val prop = new PartitioningProps.MultiStatic(extractor, keys)
      new Multi[K1, PartitioningProps.MultiStatic[K1, KS], Lambda[
        M => ParentMV[Props#MV[M]]
      ], OneForOne[
        K,
        Props,
        ParentMV,
        Parent
      ]](prop, this)
    }

    /** Creates a static multicast partition layer.
      *
      * Each input will be routed to a variable number of partitions.
      *
      * In case an input is mapped to a non existing partition key the stream will fail.
      *
      * The third parameter of the extractor function are the currently materialized partition.
      *
      * @param extractor
      *   partition key extractor function mapping each (input, context) to a set (potentially
      *   empty) of partition
      * @param keys
      *   the partition keys to materialize. MUST contain all the possible outputs of `extractor`
      * @return
      *   partition builder
      */
    def staticMulticastCtx[K1](
        extractor: (In, Ctx, Set[K1]) => Set[K1],
        keys: Set[K1]
      ): Multi[K1, PartitioningProps.MultiStatic[K1, KS], Lambda[M => ParentMV[Props#MV[M]]], OneForOne[K, Props, ParentMV, Parent]] = {
      val extractorWrapper: (In, Ctx, Set[K1], KS) => Set[K1] = (in, ctx, ks, _) =>
        extractor(in, ctx, ks)
      val keysWrapper: KS => Set[K1] = _ => keys
      staticMulticastCtxWithKeys(extractorWrapper, keysWrapper)
    }

    /** Creates a static multicast partition layer.
      *
      * Each input will be routed to a variable number of partitions.
      *
      * In case an input is mapped to a non existing partition key the stream will fail.
      *
      * The second parameter of the extractor function are the currently materialized partition.
      *
      * @param extractor
      *   partition key extractor function mapping each input to a set (potentially empty) of
      *   partition
      * @param keys
      *   the partition keys to materialize. MUST contain all the possible outputs of `extractor`
      * @return
      *   partition builder
      */
    def staticMulticast[K1](
        extractor: (In, Set[K1]) => Set[K1],
        keys: Set[K1]
      ): Multi[K1, PartitioningProps.MultiStatic[K1, KS], Lambda[M => ParentMV[Props#MV[M]]], OneForOne[K, Props, ParentMV, Parent]] = {
      staticMulticastCtx(
        (in: In, _: Ctx, keys: Set[K1]) => extractor(in, keys),
        keys
      )
    }
  }

  implicit private[PartitionTreeBuilder] def optionalCanBuildOptionalT[
      K,
      Props <: PartitioningProps.Optional[K, Parent#KS],
      ParentMV[_],
      Parent <: Layer[ParentMV],
      L <: Optional[K, Props, ParentMV, Parent]
    ](implicit ev: Layer.CanBuildOptionalT[ParentMV, Parent]
    ) =
    new Layer.CanBuildOptionalT[Lambda[M => ParentMV[Props#MV[M]]], L] {
      def buildOptional[O, M](
          layer: L,
          flowF: L#KS => FlowWithExtendedContext[In, Option[O], Ctx, M]
        ): FlowWithExtendedContext[In, Option[O], Ctx, ParentMV[Props#MV[M]]] = {
        val levelFlowF = (ks: Parent#KS) =>
          layer.props.build[Option[O], M](ks, k => flowF(k :@: ks)).map(_.flatten)
        ev.buildOptional(layer.parent, levelFlowF)
      }
    }

  implicit private[PartitionTreeBuilder] def optionalCanBuildMultiT[
      K,
      Props <: PartitioningProps.Optional[K, Parent#KS],
      ParentMV[_],
      Parent <: Layer[ParentMV],
      L <: Optional[K, Props, ParentMV, Parent]
    ](implicit ev: Layer.CanBuildOptionalT[ParentMV, Parent]
    ) =
    new Layer.CanBuildMultiT[Lambda[M => ParentMV[Props#MV[M]]], L] {
      def buildMulti[O, M](
          layer: L,
          flowF: L#KS => FlowWithExtendedContext[In, immutable.Iterable[O], Ctx, M]
        ): FlowWithExtendedContext[In, immutable.Iterable[O], Ctx, ParentMV[Props#MV[M]]] = {
        val levelFlowF = (ks: Parent#KS) =>
          layer.props.build[immutable.Iterable[O], M](ks, k => flowF(k :@: ks))
        ev
          .buildOptional(layer.parent, levelFlowF)
          .map[immutable.Iterable[O]](_.toList.flatten)
          .asInstanceOf[FlowWithExtendedContext[In, immutable.Iterable[O], Ctx, ParentMV[
            Props#MV[M]
          ]]]
      }
    }

  /** An optional partition layer.
    *
    * Given one input the layer will produce either zero or one outputs.
    */
  class Optional[
      K,
      Props <: PartitioningProps.Optional[K, Parent#KS],
      ParentMV[_],
      Parent <: Layer[ParentMV]
    ] private[spekka] (
      private[spekka] val props: Props,
      private[spekka] val parent: Parent
    )(implicit ev: Layer.CanBuildOptionalT[ParentMV, Parent])
      extends Layer[Lambda[M => ParentMV[Props#MV[M]]]] {
    override private[PartitionTreeBuilder] type KS = K :@: Parent#KS

    implicit private val canBuildOptional
        : Layer.CanBuildOptionalT[Lambda[M => ParentMV[Props#MV[M]]], Optional[K, Props, ParentMV, Parent]] =
      optionalCanBuildOptionalT[K, Props, ParentMV, Parent, Optional[K, Props, ParentMV, Parent]]
    implicit private val canBuildMulti
        : Layer.CanBuildMultiT[Lambda[M => ParentMV[Props#MV[M]]], Optional[K, Props, ParentMV, Parent]] =
      optionalCanBuildMultiT[K, Props, ParentMV, Parent, Optional[K, Props, ParentMV, Parent]]

    def build[O, M](
        flowF: KS => FlowWithExtendedContext[In, O, Ctx, M]
      ): FlowWithExtendedContext[In, Option[O], Ctx, ParentMV[Props#MV[M]]] = {
      val levelFlowF = (ks: KS) => flowF(ks).map(Some(_))

      canBuildOptional.buildOptional(
        this,
        levelFlowF
      )
    }

    /** Creates a dynamic unicast partition layer with automatic materialization.
      *
      * Each input will be routed to exactly one partition which will be automatically materialized
      * on the first element.
      *
      * @param extractor
      *   partition key extractor function mapping each (input, context, parent keys) to exactly one
      *   partition
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicAutoCtxWithKeys[K1](
        extractor: (In, Ctx, KS) => K1,
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Optional[K1, PartitioningProps.OneForOneAsOptional[K1, KS, PartitioningProps.SingleDynamicAuto[K1, KS]], Lambda[M => ParentMV[Props#MV[M]]], Optional[K, Props, ParentMV, Parent]] = {
      val baseProp =
        new PartitioningProps.SingleDynamicAuto[K1, KS](
          extractor,
          completionCriteria,
          bufferSize
        )
      val prop =
        new PartitioningProps.OneForOneAsOptional[
          K1,
          KS,
          PartitioningProps.SingleDynamicAuto[K1, KS]
        ](
          baseProp
        )
      new Optional[
        K1,
        PartitioningProps.OneForOneAsOptional[K1, KS, PartitioningProps.SingleDynamicAuto[
          K1,
          KS
        ]],
        Lambda[M => ParentMV[Props#MV[M]]],
        Optional[K, Props, ParentMV, Parent]
      ](prop, this)
    }

    /** Creates a dynamic unicast partition layer with automatic materialization.
      *
      * Each input will be routed to exactly one partition which will be automatically materialized
      * on the first element.
      *
      * @param extractor
      *   partition key extractor function mapping each (input, context) to exactly one partition
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicAutoCtx[K1](
        extractor: (In, Ctx) => K1,
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Optional[K1, PartitioningProps.OneForOneAsOptional[K1, KS, PartitioningProps.SingleDynamicAuto[K1, KS]], Lambda[M => ParentMV[Props#MV[M]]], Optional[K, Props, ParentMV, Parent]] = {
      val extractorWrapper: (In, Ctx, KS) => K1 = (in, ctx, _) => extractor(in, ctx)
      dynamicAutoCtxWithKeys(extractorWrapper, completionCriteria, bufferSize)
    }

    /** Creates a dynamic unicast partition layer with automatic materialization.
      *
      * Each input will be routed to exactly one partition which will be automatically materialized
      * on the first element.
      *
      * @param extractor
      *   partition key extractor function mapping each input to exactly one partition
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicAuto[K1](
        extractor: In => K1,
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Optional[K1, PartitioningProps.OneForOneAsOptional[K1, KS, PartitioningProps.SingleDynamicAuto[K1, KS]], Lambda[M => ParentMV[Props#MV[M]]], Optional[K, Props, ParentMV, Parent]] = {
      dynamicAutoCtx(
        (in: In, _: Ctx) => extractor(in),
        completionCriteria,
        bufferSize
      )
    }

    /** Creates a dynamic multicast partition layer with automatic materialization.
      *
      * Each input will be routed to a variable number of partitions which will be automatically
      * materialized on the first element.
      *
      * Input mapped to an empty set of partition keys are ignored, but their context is preserved.
      *
      * The third parameter of the extractor function are the currently materialized partition.
      *
      * @param extractor
      *   partition key extractor function mapping each (input, context, parent keys) to a set
      *   (potentially empty) of partitions
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicAutoMulticastCtxWithKeys[K1](
        extractor: (In, Ctx, Set[K1], KS) => Set[K1],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Multi[K1, PartitioningProps.MultiDynamicAuto[K1, KS], Lambda[M => ParentMV[Props#MV[M]]], Optional[K, Props, ParentMV, Parent]] = {
      val prop = new PartitioningProps.MultiDynamicAuto(extractor, completionCriteria, bufferSize)
      new Multi[K1, PartitioningProps.MultiDynamicAuto[K1, KS], Lambda[
        M => ParentMV[Props#MV[M]]
      ], Optional[K, Props, ParentMV, Parent]](prop, this)
    }

    /** Creates a dynamic multicast partition layer with automatic materialization.
      *
      * Each input will be routed to a variable number of partitions which will be automatically
      * materialized on the first element.
      *
      * Input mapped to an empty set of partition keys are ignored, but their context is preserved.
      *
      * The third parameter of the extractor function are the currently materialized partition.
      *
      * @param extractor
      *   partition key extractor function mapping each (input, context) to a set (potentially
      *   empty) of partitions
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicAutoMulticastCtx[K1](
        extractor: (In, Ctx, Set[K1]) => Set[K1],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Multi[K1, PartitioningProps.MultiDynamicAuto[K1, KS], Lambda[M => ParentMV[Props#MV[M]]], Optional[K, Props, ParentMV, Parent]] = {
      val extractorWrapper: (In, Ctx, Set[K1], KS) => Set[K1] = (in, ctx, ks, _) =>
        extractor(in, ctx, ks)
      dynamicAutoMulticastCtxWithKeys(extractorWrapper, completionCriteria, bufferSize)
    }

    /** Creates a dynamic multicast partition layer with automatic materialization.
      *
      * Each input will be routed to a variable number of partitions which will be automatically
      * materialized on the first element.
      *
      * Input mapped to an empty set of partition keys are ignored, but their context is preserved.
      *
      * The second parameter of the extractor function are the currently materialized partition.
      *
      * @param extractor
      *   partition key extractor function mapping each input to a set (potentially empty) of
      *   partitions
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicAutoMulticast[K1](
        extractor: (In, Set[K1]) => Set[K1],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Multi[K1, PartitioningProps.MultiDynamicAuto[K1, KS], Lambda[M => ParentMV[Props#MV[M]]], Optional[K, Props, ParentMV, Parent]] = {
      dynamicAutoMulticastCtx(
        (in: In, _: Ctx, keys: Set[K1]) => extractor(in, keys),
        completionCriteria,
        bufferSize
      )
    }

    /** Creates a dynamic unicast partition layer with manual materialization.
      *
      * Each input will be routed to exactly one partition which must be materialized manually.
      *
      * Input destined to non materialized partition keys are ignored, but their context is
      * preserved.
      *
      * @param extractor
      *   partition key extractor function mapping each (input, context, parent keys) to exactly one
      *   partition
      * @param initialKeys
      *   the set of keys to materialized when the layer is initialized
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicManualCtxWithKeys[K1](
        extractor: (In, Ctx, KS) => K1,
        initialKeys: KS => Set[K1],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Optional[K1, PartitioningProps.SingleDynamicManual[K1, KS], Lambda[M => ParentMV[Props#MV[M]]], Optional[K, Props, ParentMV, Parent]] = {
      val prop =
        new PartitioningProps.SingleDynamicManual(
          extractor,
          initialKeys,
          completionCriteria,
          bufferSize
        )
      new Optional[K1, PartitioningProps.SingleDynamicManual[K1, KS], Lambda[
        M => ParentMV[Props#MV[M]]
      ], Optional[K, Props, ParentMV, Parent]](prop, this)
    }

    /** Creates a dynamic unicast partition layer with manual materialization.
      *
      * Each input will be routed to exactly one partition which must be materialized manually.
      *
      * Input destined to non materialized partition keys are ignored, but their context is
      * preserved.
      *
      * @param extractor
      *   partition key extractor function mapping each (input, context) to exactly one partition
      * @param initialKeys
      *   the set of keys to materialized when the layer is initialized
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicManualCtx[K1](
        extractor: (In, Ctx) => K1,
        initialKeys: Set[K1],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Optional[K1, PartitioningProps.SingleDynamicManual[K1, KS], Lambda[M => ParentMV[Props#MV[M]]], Optional[K, Props, ParentMV, Parent]] = {
      val extractorWrapper: (In, Ctx, KS) => K1 = (in, ctx, _) => extractor(in, ctx)
      val initialKeysWrapper: KS => Set[K1] = _ => initialKeys
      dynamicManualCtxWithKeys(extractorWrapper, initialKeysWrapper, completionCriteria, bufferSize)
    }

    /** Creates a dynamic unicast partition layer with manual materialization.
      *
      * Each input will be routed to exactly one partition which must be materialized manually.
      *
      * Input destined to non materialized partition keys are ignored, but their context is
      * preserved.
      *
      * @param extractor
      *   partition key extractor function mapping each input to exactly one partition
      * @param initialKeys
      *   the set of keys to materialized when the layer is initialized
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicManual[K1](
        extractor: In => K1,
        initialKeys: Set[K1],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Optional[K1, PartitioningProps.SingleDynamicManual[K1, KS], Lambda[M => ParentMV[Props#MV[M]]], Optional[K, Props, ParentMV, Parent]] = {
      dynamicManualCtx(
        (in: In, _: Ctx) => extractor(in),
        initialKeys,
        completionCriteria,
        bufferSize
      )
    }

    /** Creates a dynamic multicast partition layer with manual materialization.
      *
      * Each input will be routed to a variable number of partitions which must be materialized
      * manually.
      *
      * Input destined to non materialized partition keys are ignored, but their context is
      * preserved.
      *
      * The third parameter of the extractor function are the currently materialized partition.
      *
      * @param extractor
      *   partition key extractor function mapping each (input, context, parent keys) to a set
      *   (potentially empty) of partitions
      * @param initialKeys
      *   the set of keys to materialized when the layer is initialized
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicManualMulticastCtxWithKeys[K1](
        extractor: (In, Ctx, Set[K1], KS) => Set[K1],
        initialKeys: KS => Set[K1],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Multi[K1, PartitioningProps.MultiDynamicManual[K1, KS], Lambda[M => ParentMV[Props#MV[M]]], Optional[K, Props, ParentMV, Parent]] = {
      val prop =
        new PartitioningProps.MultiDynamicManual(
          extractor,
          initialKeys,
          completionCriteria,
          bufferSize
        )
      new Multi[K1, PartitioningProps.MultiDynamicManual[K1, KS], Lambda[
        M => ParentMV[Props#MV[M]]
      ], Optional[K, Props, ParentMV, Parent]](prop, this)
    }

    /** Creates a dynamic multicast partition layer with manual materialization.
      *
      * Each input will be routed to a variable number of partitions which must be materialized
      * manually.
      *
      * Input destined to non materialized partition keys are ignored, but their context is
      * preserved.
      *
      * The third parameter of the extractor function are the currently materialized partition.
      *
      * @param extractor
      *   partition key extractor function mapping each (input, context) to a set (potentially
      *   empty) of partitions
      * @param initialKeys
      *   the set of keys to materialized when the layer is initialized
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicManualMulticastCtx[K1](
        extractor: (In, Ctx, Set[K1]) => Set[K1],
        initialKeys: Set[K1],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Multi[K1, PartitioningProps.MultiDynamicManual[K1, KS], Lambda[M => ParentMV[Props#MV[M]]], Optional[K, Props, ParentMV, Parent]] = {
      val extractorWrapper: (In, Ctx, Set[K1], KS) => Set[K1] = (in, ctx, ks, _) =>
        extractor(in, ctx, ks)
      val initialKeysWrapper: KS => Set[K1] = _ => initialKeys
      dynamicManualMulticastCtxWithKeys(
        extractorWrapper,
        initialKeysWrapper,
        completionCriteria,
        bufferSize
      )
    }

    /** Creates a dynamic multicast partition layer with manual materialization.
      *
      * Each input will be routed to a variable number of partitions which must be materialized
      * manually.
      *
      * Input destined to non materialized partition keys are ignored, but their context is
      * preserved.
      *
      * The second parameter of the extractor function are the currently materialized partition.
      *
      * @param extractor
      *   partition key extractor function mapping each input to a set (potentially empty) of
      *   partitions
      * @param initialKeys
      *   the set of keys to materialized when the layer is initialized
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicManualMulticast[K1](
        extractor: (In, Set[K1]) => Set[K1],
        initialKeys: Set[K1],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Multi[K1, PartitioningProps.MultiDynamicManual[K1, KS], Lambda[M => ParentMV[Props#MV[M]]], Optional[K, Props, ParentMV, Parent]] = {
      dynamicManualMulticastCtx(
        (in: In, _: Ctx, keys: Set[K1]) => extractor(in, keys),
        initialKeys,
        completionCriteria,
        bufferSize
      )
    }

    /** Creates a static unicast partition layer.
      *
      * Each input will be routed to exactly one partition.
      *
      * In case an input is mapped to a non existing partition key the stream will fail.
      *
      * @param extractor
      *   partition key extractor function mapping each (input, context, parent keys) to exactly one
      *   partition
      * @param keys
      *   the partition keys to materialize. MUST contain all the possible outputs of `extractor`
      * @return
      *   partition builder
      */
    def staticCtxWithKeys[K1](
        extractor: (In, Ctx, KS) => K1,
        keys: KS => Set[K1]
      ): Optional[K1, PartitioningProps.OneForOneAsOptional[K1, KS, PartitioningProps.SingleStatic[K1, KS]], Lambda[M => ParentMV[Props#MV[M]]], Optional[K, Props, ParentMV, Parent]] = {
      val baseProp = new PartitioningProps.SingleStatic(extractor, keys)
      val prop =
        new PartitioningProps.OneForOneAsOptional[
          K1,
          KS,
          PartitioningProps.SingleStatic[K1, KS]
        ](baseProp)
      new Optional[
        K1,
        PartitioningProps.OneForOneAsOptional[
          K1,
          KS,
          PartitioningProps.SingleStatic[K1, KS]
        ],
        Lambda[M => ParentMV[Props#MV[M]]],
        Optional[K, Props, ParentMV, Parent]
      ](prop, this)
    }

    /** Creates a static unicast partition layer.
      *
      * Each input will be routed to exactly one partition.
      *
      * In case an input is mapped to a non existing partition key the stream will fail.
      *
      * @param extractor
      *   partition key extractor function mapping each (input, context) to exactly one partition
      * @param keys
      *   the partition keys to materialize. MUST contain all the possible outputs of `extractor`
      * @return
      *   partition builder
      */
    def staticCtx[K1](
        extractor: (In, Ctx) => K1,
        keys: Set[K1]
      ): Optional[K1, PartitioningProps.OneForOneAsOptional[K1, KS, PartitioningProps.SingleStatic[K1, KS]], Lambda[M => ParentMV[Props#MV[M]]], Optional[K, Props, ParentMV, Parent]] = {
      val extractorWrapper: (In, Ctx, KS) => K1 = (in, ctx, _) => extractor(in, ctx)
      val keysWrapper: KS => Set[K1] = _ => keys
      staticCtxWithKeys(extractorWrapper, keysWrapper)
    }

    /** Creates a static unicast partition layer.
      *
      * Each input will be routed to exactly one partition.
      *
      * In case an input is mapped to a non existing partition key the stream will fail.
      *
      * @param extractor
      *   partition key extractor function mapping each input to exactly one partition
      * @param keys
      *   the partition keys to materialize. MUST contain all the possible outputs of `extractor`
      * @return
      *   partition builder
      */
    def static[K1](
        extractor: In => K1,
        keys: Set[K1]
      ): Optional[K1, PartitioningProps.OneForOneAsOptional[K1, KS, PartitioningProps.SingleStatic[K1, KS]], Lambda[M => ParentMV[Props#MV[M]]], Optional[K, Props, ParentMV, Parent]] = {
      staticCtx(
        (in: In, _: Ctx) => extractor(in),
        keys
      )
    }

    /** Creates a static multicast partition layer.
      *
      * Each input will be routed to a variable number of partitions.
      *
      * In case an input is mapped to a non existing partition key the stream will fail.
      *
      * The third parameter of the extractor function are the currently materialized partition.
      *
      * @param extractor
      *   partition key extractor function mapping each (input, context, parent keys) to a set
      *   (potentially empty) of partition
      * @param keys
      *   the partition keys to materialize. MUST contain all the possible outputs of `extractor`
      * @return
      *   partition builder
      */
    def staticMulticastCtxWithKeys[K1](
        extractor: (In, Ctx, Set[K1], KS) => Set[K1],
        keys: KS => Set[K1]
      ): Multi[K1, PartitioningProps.MultiStatic[K1, KS], Lambda[M => ParentMV[Props#MV[M]]], Optional[K, Props, ParentMV, Parent]] = {
      val prop = new PartitioningProps.MultiStatic(extractor, keys)
      new Multi[K1, PartitioningProps.MultiStatic[K1, KS], Lambda[
        M => ParentMV[Props#MV[M]]
      ], Optional[
        K,
        Props,
        ParentMV,
        Parent
      ]](prop, this)
    }

    /** Creates a static multicast partition layer.
      *
      * Each input will be routed to a variable number of partitions.
      *
      * In case an input is mapped to a non existing partition key the stream will fail.
      *
      * The third parameter of the extractor function are the currently materialized partition.
      *
      * @param extractor
      *   partition key extractor function mapping each (input, context) to a set (potentially
      *   empty) of partition
      * @param keys
      *   the partition keys to materialize. MUST contain all the possible outputs of `extractor`
      * @return
      *   partition builder
      */
    def staticMulticastCtx[K1](
        extractor: (In, Ctx, Set[K1]) => Set[K1],
        keys: Set[K1]
      ): Multi[K1, PartitioningProps.MultiStatic[K1, KS], Lambda[M => ParentMV[Props#MV[M]]], Optional[K, Props, ParentMV, Parent]] = {
      val extractorWrapper: (In, Ctx, Set[K1], KS) => Set[K1] = (in, ctx, ks, _) =>
        extractor(in, ctx, ks)
      val keysWrapper: KS => Set[K1] = _ => keys
      staticMulticastCtxWithKeys(extractorWrapper, keysWrapper)
    }

    /** Creates a static multicast partition layer.
      *
      * Each input will be routed to a variable number of partitions.
      *
      * In case an input is mapped to a non existing partition key the stream will fail.
      *
      * The second parameter of the extractor function are the currently materialized partition.
      *
      * @param extractor
      *   partition key extractor function mapping each input to a set (potentially empty) of
      *   partition
      * @param keys
      *   the partition keys to materialize. MUST contain all the possible outputs of `extractor`
      * @return
      *   partition builder
      */
    def staticMulticast[K1](
        extractor: (In, Set[K1]) => Set[K1],
        keys: Set[K1]
      ): Multi[K1, PartitioningProps.MultiStatic[K1, KS], Lambda[M => ParentMV[Props#MV[M]]], Optional[K, Props, ParentMV, Parent]] = {
      staticMulticastCtx(
        (in: In, _: Ctx, keys: Set[K1]) => extractor(in, keys),
        keys
      )
    }
  }

  implicit private[PartitionTreeBuilder] def multiCanBuildMultiT[
      K,
      Props <: PartitioningProps.Multi[K, Parent#KS],
      ParentMV[_],
      Parent <: Layer[ParentMV],
      L <: Multi[K, Props, ParentMV, Parent]
    ](implicit ev: Layer.CanBuildMultiT[ParentMV, Parent]
    ) =
    new Layer.CanBuildMultiT[Lambda[M => ParentMV[Props#MV[M]]], L] {
      def buildMulti[O, M](
          layer: L,
          flowF: L#KS => FlowWithExtendedContext[In, immutable.Iterable[O], Ctx, M]
        ): FlowWithExtendedContext[In, immutable.Iterable[O], Ctx, ParentMV[Props#MV[M]]] = {
        val levelFlowF = (ks: Parent#KS) =>
          layer.props.build[immutable.Iterable[O], M](ks, k => flowF(k :@: ks)).map(_.flatten)
        ev.buildMulti(layer.parent, levelFlowF)
      }
    }

  /** A multi partition layer.
    *
    * Given one input the layer will produce either zero or n outputs.
    */
  class Multi[
      K,
      Props <: PartitioningProps.Multi[K, Parent#KS],
      ParentMV[_],
      Parent <: Layer[ParentMV]
    ] private[spekka] (
      private[spekka] val props: Props,
      private[spekka] val parent: Parent
    )(implicit ev: Layer.CanBuildMultiT[ParentMV, Parent])
      extends Layer[Lambda[M => ParentMV[Props#MV[M]]]] {
    override private[PartitionTreeBuilder] type KS = K :@: Parent#KS

    implicit private val canBuildMulti
        : Layer.CanBuildMultiT[Lambda[M => ParentMV[Props#MV[M]]], Multi[K, Props, ParentMV, Parent]] =
      multiCanBuildMultiT[K, Props, ParentMV, Parent, Multi[K, Props, ParentMV, Parent]]

    def build[O, M](
        flowF: KS => FlowWithExtendedContext[In, O, Ctx, M]
      ): FlowWithExtendedContext[In, immutable.Iterable[O], Ctx, ParentMV[Props#MV[M]]] = {
      val levelFlowF = (ks: KS) => flowF(ks).map(List(_))

      canBuildMulti.buildMulti(
        this,
        levelFlowF
      )
    }

    /** Creates a dynamic unicast partition layer with automatic materialization.
      *
      * Each input will be routed to exactly one partition which will be automatically materialized
      * on the first element.
      *
      * @param extractor
      *   partition key extractor function mapping each (input, context, parent keys) to exactly one
      *   partition
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicAutoCtxWithKeys[K1](
        extractor: (In, Ctx, KS) => K1,
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Multi[K1, PartitioningProps.OneForOneAsMulti[K1, KS, PartitioningProps.SingleDynamicAuto[K1, KS]], Lambda[M => ParentMV[Props#MV[M]]], Multi[K, Props, ParentMV, Parent]] = {
      val baseProp =
        new PartitioningProps.SingleDynamicAuto[K1, KS](
          extractor,
          completionCriteria,
          bufferSize
        )
      val prop =
        new PartitioningProps.OneForOneAsMulti[
          K1,
          KS,
          PartitioningProps.SingleDynamicAuto[K1, KS]
        ](
          baseProp
        )
      new Multi[
        K1,
        PartitioningProps.OneForOneAsMulti[
          K1,
          KS,
          PartitioningProps.SingleDynamicAuto[K1, KS]
        ],
        Lambda[M => ParentMV[Props#MV[M]]],
        Multi[K, Props, ParentMV, Parent]
      ](prop, this)
    }

    /** Creates a dynamic unicast partition layer with automatic materialization.
      *
      * Each input will be routed to exactly one partition which will be automatically materialized
      * on the first element.
      *
      * @param extractor
      *   partition key extractor function mapping each (input, context) to exactly one partition
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicAutoCtx[K1](
        extractor: (In, Ctx) => K1,
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Multi[K1, PartitioningProps.OneForOneAsMulti[K1, KS, PartitioningProps.SingleDynamicAuto[K1, KS]], Lambda[M => ParentMV[Props#MV[M]]], Multi[K, Props, ParentMV, Parent]] = {
      val extractorWrapper: (In, Ctx, KS) => K1 = (in, ctx, _) => extractor(in, ctx)
      dynamicAutoCtxWithKeys(extractorWrapper, completionCriteria, bufferSize)
    }

    /** Creates a dynamic unicast partition layer with automatic materialization.
      *
      * Each input will be routed to exactly one partition which will be automatically materialized
      * on the first element.
      *
      * @param extractor
      *   partition key extractor function mapping each input to exactly one partition
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicAuto[K1](
        extractor: In => K1,
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Multi[K1, PartitioningProps.OneForOneAsMulti[K1, KS, PartitioningProps.SingleDynamicAuto[K1, KS]], Lambda[M => ParentMV[Props#MV[M]]], Multi[K, Props, ParentMV, Parent]] = {
      dynamicAutoCtx(
        (in: In, _: Ctx) => extractor(in),
        completionCriteria,
        bufferSize
      )
    }

    /** Creates a dynamic multicast partition layer with automatic materialization.
      *
      * Each input will be routed to a variable number of partitions which will be automatically
      * materialized on the first element.
      *
      * Input mapped to an empty set of partition keys are ignored, but their context is preserved.
      *
      * The third parameter of the extractor function are the currently materialized partition.
      *
      * @param extractor
      *   partition key extractor function mapping each (input, context, parent keys) to a set
      *   (potentially empty) of partitions
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicAutoMulticastCtxWithKeys[K1](
        extractor: (In, Ctx, Set[K1], KS) => Set[K1],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Multi[K1, PartitioningProps.MultiDynamicAuto[K1, KS], Lambda[M => ParentMV[Props#MV[M]]], Multi[K, Props, ParentMV, Parent]] = {
      val prop = new PartitioningProps.MultiDynamicAuto(extractor, completionCriteria, bufferSize)
      new Multi[K1, PartitioningProps.MultiDynamicAuto[K1, KS], Lambda[
        M => ParentMV[Props#MV[M]]
      ], Multi[
        K,
        Props,
        ParentMV,
        Parent
      ]](prop, this)
    }

    /** Creates a dynamic multicast partition layer with automatic materialization.
      *
      * Each input will be routed to a variable number of partitions which will be automatically
      * materialized on the first element.
      *
      * Input mapped to an empty set of partition keys are ignored, but their context is preserved.
      *
      * The third parameter of the extractor function are the currently materialized partition.
      *
      * @param extractor
      *   partition key extractor function mapping each (input, context) to a set (potentially
      *   empty) of partitions
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicAutoMulticastCtx[K1](
        extractor: (In, Ctx, Set[K1]) => Set[K1],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Multi[K1, PartitioningProps.MultiDynamicAuto[K1, KS], Lambda[M => ParentMV[Props#MV[M]]], Multi[K, Props, ParentMV, Parent]] = {
      val extractorWrapper: (In, Ctx, Set[K1], KS) => Set[K1] = (in, ctx, ks, _) =>
        extractor(in, ctx, ks)
      dynamicAutoMulticastCtxWithKeys(extractorWrapper, completionCriteria, bufferSize)
    }

    /** Creates a dynamic multicast partition layer with automatic materialization.
      *
      * Each input will be routed to a variable number of partitions which will be automatically
      * materialized on the first element.
      *
      * Input mapped to an empty set of partition keys are ignored, but their context is preserved.
      *
      * The second parameter of the extractor function are the currently materialized partition.
      *
      * @param extractor
      *   partition key extractor function mapping each input to a set (potentially empty) of
      *   partitions
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicAutoMulticast[K1](
        extractor: (In, Set[K1]) => Set[K1],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Multi[K1, PartitioningProps.MultiDynamicAuto[K1, KS], Lambda[M => ParentMV[Props#MV[M]]], Multi[K, Props, ParentMV, Parent]] = {
      dynamicAutoMulticastCtx(
        (in: In, _: Ctx, keys: Set[K1]) => extractor(in, keys),
        completionCriteria,
        bufferSize
      )
    }

    /** Creates a dynamic unicast partition layer with manual materialization.
      *
      * Each input will be routed to exactly one partition which must be materialized manually.
      *
      * Input destined to non materialized partition keys are ignored, but their context is
      * preserved.
      *
      * @param extractor
      *   partition key extractor function mapping each (input, context, parent keys) to exactly one
      *   partition
      * @param initialKeys
      *   the set of keys to materialized when the layer is initialized
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicManualCtxWithKeys[K1](
        extractor: (In, Ctx, KS) => K1,
        initialKeys: KS => Set[K1],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Multi[K1, PartitioningProps.OptionalAsMulti[K1, KS, PartitioningProps.SingleDynamicManual[K1, KS]], Lambda[M => ParentMV[Props#MV[M]]], Multi[K, Props, ParentMV, Parent]] = {
      val baseProp =
        new PartitioningProps.SingleDynamicManual(
          extractor,
          initialKeys,
          completionCriteria,
          bufferSize
        )
      val prop =
        new PartitioningProps.OptionalAsMulti[
          K1,
          KS,
          PartitioningProps.SingleDynamicManual[K1, KS]
        ](
          baseProp
        )
      new Multi[
        K1,
        PartitioningProps.OptionalAsMulti[
          K1,
          KS,
          PartitioningProps.SingleDynamicManual[K1, KS]
        ],
        Lambda[M => ParentMV[Props#MV[M]]],
        Multi[K, Props, ParentMV, Parent]
      ](prop, this)
    }

    /** Creates a dynamic unicast partition layer with manual materialization.
      *
      * Each input will be routed to exactly one partition which must be materialized manually.
      *
      * Input destined to non materialized partition keys are ignored, but their context is
      * preserved.
      *
      * @param extractor
      *   partition key extractor function mapping each (input, context) to exactly one partition
      * @param initialKeys
      *   the set of keys to materialized when the layer is initialized
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicManualCtx[K1](
        extractor: (In, Ctx) => K1,
        initialKeys: Set[K1],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Multi[K1, PartitioningProps.OptionalAsMulti[K1, KS, PartitioningProps.SingleDynamicManual[K1, KS]], Lambda[M => ParentMV[Props#MV[M]]], Multi[K, Props, ParentMV, Parent]] = {
      val extractorWrapper: (In, Ctx, KS) => K1 = (in, ctx, _) => extractor(in, ctx)
      val initialKeysWrapper: KS => Set[K1] = _ => initialKeys
      dynamicManualCtxWithKeys(extractorWrapper, initialKeysWrapper, completionCriteria, bufferSize)
    }

    /** Creates a dynamic unicast partition layer with manual materialization.
      *
      * Each input will be routed to exactly one partition which must be materialized manually.
      *
      * Input destined to non materialized partition keys are ignored, but their context is
      * preserved.
      *
      * @param extractor
      *   partition key extractor function mapping each input to exactly one partition
      * @param initialKeys
      *   the set of keys to materialized when the layer is initialized
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicManual[K1](
        extractor: In => K1,
        initialKeys: Set[K1],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Multi[K1, PartitioningProps.OptionalAsMulti[K1, KS, PartitioningProps.SingleDynamicManual[K1, KS]], Lambda[M => ParentMV[Props#MV[M]]], Multi[K, Props, ParentMV, Parent]] = {
      dynamicManualCtx(
        (in: In, _: Ctx) => extractor(in),
        initialKeys,
        completionCriteria,
        bufferSize
      )
    }

    /** Creates a dynamic multicast partition layer with manual materialization.
      *
      * Each input will be routed to a variable number of partitions which must be materialized
      * manually.
      *
      * Input destined to non materialized partition keys are ignored, but their context is
      * preserved.
      *
      * The third parameter of the extractor function are the currently materialized partition.
      *
      * @param extractor
      *   partition key extractor function mapping each (input, context, parent keys) to a set
      *   (potentially empty) of partitions
      * @param initialKeys
      *   the set of keys to materialized when the layer is initialized
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicManualMulticastCtxWithKeys[K1](
        extractor: (In, Ctx, Set[K1], KS) => Set[K1],
        initialKeys: KS => Set[K1],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Multi[K1, PartitioningProps.MultiDynamicManual[K1, KS], Lambda[M => ParentMV[Props#MV[M]]], Multi[K, Props, ParentMV, Parent]] = {
      val prop =
        new PartitioningProps.MultiDynamicManual(
          extractor,
          initialKeys,
          completionCriteria,
          bufferSize
        )
      new Multi[K1, PartitioningProps.MultiDynamicManual[K1, KS], Lambda[
        M => ParentMV[Props#MV[M]]
      ], Multi[
        K,
        Props,
        ParentMV,
        Parent
      ]](prop, this)
    }

    /** Creates a dynamic multicast partition layer with manual materialization.
      *
      * Each input will be routed to a variable number of partitions which must be materialized
      * manually.
      *
      * Input destined to non materialized partition keys are ignored, but their context is
      * preserved.
      *
      * The third parameter of the extractor function are the currently materialized partition.
      *
      * @param extractor
      *   partition key extractor function mapping each (input, context) to a set (potentially
      *   empty) of partitions
      * @param initialKeys
      *   the set of keys to materialized when the layer is initialized
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicManualMulticastCtx[K1](
        extractor: (In, Ctx, Set[K1]) => Set[K1],
        initialKeys: Set[K1],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Multi[K1, PartitioningProps.MultiDynamicManual[K1, KS], Lambda[M => ParentMV[Props#MV[M]]], Multi[K, Props, ParentMV, Parent]] = {
      val extractorWrapper: (In, Ctx, Set[K1], KS) => Set[K1] = (in, ctx, ks, _) =>
        extractor(in, ctx, ks)
      val initialKeysWrapper: KS => Set[K1] = _ => initialKeys
      dynamicManualMulticastCtxWithKeys(
        extractorWrapper,
        initialKeysWrapper,
        completionCriteria,
        bufferSize
      )
    }

    /** Creates a dynamic multicast partition layer with manual materialization.
      *
      * Each input will be routed to a variable number of partitions which must be materialized
      * manually.
      *
      * Input destined to non materialized partition keys are ignored, but their context is
      * preserved.
      *
      * The second parameter of the extractor function are the currently materialized partition.
      *
      * @param extractor
      *   partition key extractor function mapping each input to a set (potentially empty) of
      *   partitions
      * @param initialKeys
      *   the set of keys to materialized when the layer is initialized
      * @param completionCriteria
      *   completion criteria for the materialized partition handlers
      * @param bufferSize
      *   elements buffer of the partition layer
      * @return
      *   partition builder
      */
    def dynamicManualMulticast[K1](
        extractor: (In, Set[K1]) => Set[K1],
        initialKeys: Set[K1],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Multi[K1, PartitioningProps.MultiDynamicManual[K1, KS], Lambda[M => ParentMV[Props#MV[M]]], Multi[K, Props, ParentMV, Parent]] = {
      dynamicManualMulticastCtx(
        (in: In, _: Ctx, keys: Set[K1]) => extractor(in, keys),
        initialKeys,
        completionCriteria,
        bufferSize
      )
    }

    /** Creates a static unicast partition layer.
      *
      * Each input will be routed to exactly one partition.
      *
      * In case an input is mapped to a non existing partition key the stream will fail.
      *
      * @param extractor
      *   partition key extractor function mapping each (input, context, parent keys) to exactly one
      *   partition
      * @param keys
      *   the partition keys to materialize. MUST contain all the possible outputs of `extractor`
      * @return
      *   partition builder
      */
    def staticCtxWithKeys[K1](
        extractor: (In, Ctx, KS) => K1,
        keys: KS => Set[K1]
      ): Multi[K1, PartitioningProps.OneForOneAsMulti[K1, KS, PartitioningProps.SingleStatic[K1, KS]], Lambda[M => ParentMV[Props#MV[M]]], Multi[K, Props, ParentMV, Parent]] = {
      val baseProp = new PartitioningProps.SingleStatic(extractor, keys)
      val prop =
        new PartitioningProps.OneForOneAsMulti[K1, KS, PartitioningProps.SingleStatic[K1, KS]](
          baseProp
        )
      new Multi[
        K1,
        PartitioningProps.OneForOneAsMulti[K1, KS, PartitioningProps.SingleStatic[K1, KS]],
        Lambda[
          M => ParentMV[Props#MV[M]]
        ],
        Multi[K, Props, ParentMV, Parent]
      ](prop, this)
    }

    /** Creates a static unicast partition layer.
      *
      * Each input will be routed to exactly one partition.
      *
      * In case an input is mapped to a non existing partition key the stream will fail.
      *
      * @param extractor
      *   partition key extractor function mapping each (input, context) to exactly one partition
      * @param keys
      *   the partition keys to materialize. MUST contain all the possible outputs of `extractor`
      * @return
      *   partition builder
      */
    def staticCtx[K1](
        extractor: (In, Ctx) => K1,
        keys: Set[K1]
      ): Multi[K1, PartitioningProps.OneForOneAsMulti[K1, KS, PartitioningProps.SingleStatic[K1, KS]], Lambda[M => ParentMV[Props#MV[M]]], Multi[K, Props, ParentMV, Parent]] = {
      val extractorWrapper: (In, Ctx, KS) => K1 = (in, ctx, _) => extractor(in, ctx)
      val keysWrapper: KS => Set[K1] = _ => keys
      staticCtxWithKeys(extractorWrapper, keysWrapper)
    }

    /** Creates a static unicast partition layer.
      *
      * Each input will be routed to exactly one partition.
      *
      * In case an input is mapped to a non existing partition key the stream will fail.
      *
      * @param extractor
      *   partition key extractor function mapping each input to exactly one partition
      * @param keys
      *   the partition keys to materialize. MUST contain all the possible outputs of `extractor`
      * @return
      *   partition builder
      */
    def static[K1](
        extractor: In => K1,
        keys: Set[K1]
      ): Multi[K1, PartitioningProps.OneForOneAsMulti[K1, KS, PartitioningProps.SingleStatic[K1, KS]], Lambda[M => ParentMV[Props#MV[M]]], Multi[K, Props, ParentMV, Parent]] = {
      staticCtx(
        (in: In, _: Ctx) => extractor(in),
        keys
      )
    }

    /** Creates a static multicast partition layer.
      *
      * Each input will be routed to a variable number of partitions.
      *
      * In case an input is mapped to a non existing partition key the stream will fail.
      *
      * The third parameter of the extractor function are the currently materialized partition.
      *
      * @param extractor
      *   partition key extractor function mapping each (input, context, parent keys) to a set
      *   (potentially empty) of partition
      * @param keys
      *   the partition keys to materialize. MUST contain all the possible outputs of `extractor`
      * @return
      *   partition builder
      */
    def staticMulticastCtxWithKeys[K1](
        extractor: (In, Ctx, Set[K1], KS) => Set[K1],
        keys: KS => Set[K1]
      ): Multi[K1, PartitioningProps.MultiStatic[K1, KS], Lambda[M => ParentMV[Props#MV[M]]], Multi[K, Props, ParentMV, Parent]] = {
      val prop = new PartitioningProps.MultiStatic(extractor, keys)
      new Multi[K1, PartitioningProps.MultiStatic[K1, KS], Lambda[
        M => ParentMV[Props#MV[M]]
      ], Multi[
        K,
        Props,
        ParentMV,
        Parent
      ]](prop, this)
    }

    /** Creates a static multicast partition layer.
      *
      * Each input will be routed to a variable number of partitions.
      *
      * In case an input is mapped to a non existing partition key the stream will fail.
      *
      * The third parameter of the extractor function are the currently materialized partition.
      *
      * @param extractor
      *   partition key extractor function mapping each (input, context) to a set (potentially
      *   empty) of partition
      * @param keys
      *   the partition keys to materialize. MUST contain all the possible outputs of `extractor`
      * @return
      *   partition builder
      */
    def staticMulticastCtx[K1](
        extractor: (In, Ctx, Set[K1]) => Set[K1],
        keys: Set[K1]
      ): Multi[K1, PartitioningProps.MultiStatic[K1, KS], Lambda[M => ParentMV[Props#MV[M]]], Multi[K, Props, ParentMV, Parent]] = {
      val extractorWrapper: (In, Ctx, Set[K1], KS) => Set[K1] = (in, ctx, ks, _) =>
        extractor(in, ctx, ks)
      val keysWrapper: KS => Set[K1] = _ => keys
      staticMulticastCtxWithKeys(extractorWrapper, keysWrapper)
    }

    /** Creates a static multicast partition layer.
      *
      * Each input will be routed to a variable number of partitions.
      *
      * In case an input is mapped to a non existing partition key the stream will fail.
      *
      * The second parameter of the extractor function are the currently materialized partition.
      *
      * @param extractor
      *   partition key extractor function mapping each input to a set (potentially empty) of
      *   partition
      * @param keys
      *   the partition keys to materialize. MUST contain all the possible outputs of `extractor`
      * @return
      *   partition builder
      */
    def staticMulticast[K1](
        extractor: (In, Set[K1]) => Set[K1],
        keys: Set[K1]
      ): Multi[K1, PartitioningProps.MultiStatic[K1, KS], Lambda[M => ParentMV[Props#MV[M]]], Multi[K, Props, ParentMV, Parent]] = {
      staticMulticastCtx(
        (in: In, _: Ctx, keys: Set[K1]) => extractor(in, keys),
        keys
      )
    }
  }
}
