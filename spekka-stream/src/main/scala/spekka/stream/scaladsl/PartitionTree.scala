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

package spekka.stream.scaladsl

import akka.Done

import scala.collection.immutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future

object PartitionTree {
  sealed trait KSeq
  object KSeq {
    implicit class KSeqOpts[KS <: KSeq](kseq: KS) {
      def ::[H](h: H): H :: KS = new ::(h, kseq)
    }
  }
  final case class ::[H, T <: KSeq](head: H, tail: T) extends KSeq
  sealed trait KNil extends KSeq {
    def ::[H](h: H): H :: KNil = new ::(h, this)
  }
  case object KNil extends KNil

  sealed trait PartitionControl
  object PartitionControl {
    final class StaticControl[K, M](layer: Map[K, M]) extends PartitionControl {
      def atKey(k: K): PControlResult[M] = PControlResult(_ => Future.successful(layer.get(k)))
    }

    final class DynamicControl[K, M](layer: PartitionDynamic.Control[K, M])
        extends PartitionControl {
      def atKey(k: K): PControlResult[M] =
        PControlResult(_ => layer.withKeyMaterializedValue(k)(identity))

      def atKeyForced(k: K): PControlResult[M] =
        PControlResult { ec: ExecutionContext =>
          layer
            .withKeyMaterializedValue(k)(identity)
            .flatMap {
              case Some(v) => Future.successful(Some(v))
              case None => layer.materializeKey(k).map(Some(_))(ec)
            }(ec)
        }

      def completeKey(k: K): PControlResult[Done] =
        PControlResult(ec => layer.completeKey(k).map(Some(_))(ec))

      def materializeKey(k: K): PControlResult[M] =
        PControlResult(ec => layer.materializeKey(k).map(Some(_))(ec))

      def rematerializeKey(k: K): PControlResult[(Option[M], M)] =
        PControlResult(ec => layer.rematerializeKey(k).map(Some(_))(ec))
    }

    case class PControlResult[T](resultF: ExecutionContext => Future[Option[T]]) {
      def map[U](f: T => U): PControlResult[U] = {
        val resultF1 = (e: ExecutionContext) => resultF(e).map(_.map(f))(e)
        PControlResult(resultF1)
      }

      def flatMap[U](f: T => PControlResult[U]): PControlResult[U] = {
        val resultF1 = (e: ExecutionContext) =>
          resultF(e).flatMap {
            case Some(result) => f(result).run(e)
            case None => Future.successful(None)
          }(e)

        PControlResult(resultF1)
      }

      def run(implicit ec: ExecutionContext): Future[Option[T]] = {
        resultF(ec)
      }
    }
  }
}

class PartitionTreeBuilder[In, Ctx] private[spekka] {
  import PartitionTree._

  sealed trait PartitioningProps[K] {
    private[PartitionTreeBuilder] type MV[M]
    private[PartitionTreeBuilder] type FOut[O]

    def build[O, M](
        flowF: K => FlowWithExtendedContext[In, O, Ctx, M]
      ): FlowWithExtendedContext[In, FOut[O], Ctx, MV[M]]
  }

  object PartitioningProps {
    sealed trait OneForOne[K] extends PartitioningProps[K] {
      override private[PartitionTreeBuilder] type FOut[O] = O
    }

    sealed trait Optional[K] extends PartitioningProps[K] {
      override private[PartitionTreeBuilder] type FOut[O] = Option[O]
    }

    case class OneForOneAsOptional[K, P <: OneForOne[K]](oneForOne: P) extends Optional[K] {
      override private[PartitionTreeBuilder] type MV[M] = P#MV[M]
      override def build[O, M](
          flowF: K => FlowWithExtendedContext[In, O, Ctx, M]
        ): FlowWithExtendedContext[In, FOut[O], Ctx, MV[M]] = {
        oneForOne.build[O, M](k => flowF(k)).map(Some(_))
      }
    }

    sealed trait Multi[K] extends PartitioningProps[K] {
      override private[PartitionTreeBuilder] type FOut[O] = immutable.Iterable[O]
    }

    case class OneForOneAsMulti[K, P <: OneForOne[K]](oneForOne: P) extends Multi[K] {
      override private[PartitionTreeBuilder] type MV[M] = P#MV[M]
      override def build[O, M](
          flowF: K => FlowWithExtendedContext[In, O, Ctx, M]
        ): FlowWithExtendedContext[In, FOut[O], Ctx, MV[M]] = {
        oneForOne.build[O, M](k => flowF(k)).map(List(_))
      }
    }

    case class OptionalAsMulti[K, P <: Optional[K]](optional: P) extends Multi[K] {
      override private[PartitionTreeBuilder] type MV[M] = P#MV[M]
      override def build[O, M](
          flowF: K => FlowWithExtendedContext[In, O, Ctx, M]
        ): FlowWithExtendedContext[In, FOut[O], Ctx, MV[M]] = {
        optional.build[O, M](k => flowF(k)).map(_.toList)
      }
    }

    case class SingleDynamicAuto[K](
        extractor: (In, Ctx) => K,
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx],
        bufferSize: Int)
        extends OneForOne[K] {
      override private[PartitionTreeBuilder] type MV[M] = PartitionControl.DynamicControl[K, M]
      override def build[O, M](
          flowF: K => FlowWithExtendedContext[In, O, Ctx, M]
        ): FlowWithExtendedContext[In, FOut[O], Ctx, MV[M]] =
        Partition
          .dynamic(extractor, flowF, completionCriteria, bufferSize)
          .mapMaterializedValue(new PartitionControl.DynamicControl[K, M](_))
    }

    case class SingleDynamicManual[K](
        extractor: (In, Ctx) => K,
        initialKeys: Set[K],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx],
        bufferSize: Int)
        extends Optional[K] {
      override private[PartitionTreeBuilder] type MV[M] = PartitionControl.DynamicControl[K, M]
      override def build[O, M](
          flowF: K => FlowWithExtendedContext[In, O, Ctx, M]
        ): FlowWithExtendedContext[In, FOut[O], Ctx, MV[M]] = {
        val optionalFlowF = (k: K) => flowF(k).map(Some(_))
        val passthroughFlow = FlowWithExtendedContext[In, Ctx].map(_ => None)
        Partition
          .dynamicManual(
            extractor,
            optionalFlowF,
            passthroughFlow,
            completionCriteria,
            initialKeys,
            bufferSize
          )
          .mapMaterializedValue(new PartitionControl.DynamicControl[K, M](_))
      }
    }

    case class MultiDynamicAuto[K](
        extractor: (In, Ctx, Set[K]) => Set[K],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx],
        bufferSize: Int)
        extends Multi[K] {
      override private[PartitionTreeBuilder] type MV[M] = PartitionControl.DynamicControl[K, M]
      override def build[O, M](
          flowF: K => FlowWithExtendedContext[In, O, Ctx, M]
        ): FlowWithExtendedContext[In, FOut[O], Ctx, MV[M]] = {
        val optionalFlowF = (k: K) => flowF(k).map(Some(_))
        val passthroughFlow = FlowWithExtendedContext[In, Ctx].map(_ => None)
        Partition
          .dynamicMulti(extractor, optionalFlowF, passthroughFlow, completionCriteria, bufferSize)
          .map(_.flatten)
          .mapMaterializedValue(new PartitionControl.DynamicControl[K, M](_))
      }
    }

    case class MultiDynamicManual[K](
        extractor: (In, Ctx, Set[K]) => Set[K],
        initialKeys: Set[K],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx],
        bufferSize: Int)
        extends Multi[K] {
      override private[PartitionTreeBuilder] type MV[M] = PartitionControl.DynamicControl[K, M]
      override def build[O, M](
          flowF: K => FlowWithExtendedContext[In, O, Ctx, M]
        ): FlowWithExtendedContext[In, FOut[O], Ctx, MV[M]] = {
        val optionalFlowF = (k: K) => flowF(k).map(Some(_))
        val passthroughFlow = FlowWithExtendedContext[In, Ctx].map(_ => None)
        Partition
          .dynamicMultiManual(
            extractor,
            optionalFlowF,
            passthroughFlow,
            completionCriteria,
            initialKeys,
            bufferSize
          )
          .map(_.flatten)
          .mapMaterializedValue(new PartitionControl.DynamicControl[K, M](_))
      }
    }

    case class SingleStatic[K](
        extractor: (In, Ctx) => K,
        keys: Set[K])
        extends OneForOne[K] {
      override private[PartitionTreeBuilder] type MV[M] = PartitionControl.StaticControl[K, M]
      override def build[O, M](
          flowF: K => FlowWithExtendedContext[In, O, Ctx, M]
        ): FlowWithExtendedContext[In, FOut[O], Ctx, MV[M]] = {
        val passthroughFlow = FlowWithExtendedContext[In, Ctx].map(in =>
          throw new IllegalArgumentException(s"No flow defined SingleStatic router input: ${in}")
        )
        val flowMap = keys.iterator.map(k => k -> flowF(k)).toMap
        Partition
          .static(extractor, flowMap, passthroughFlow)
          .mapMaterializedValue(new PartitionControl.StaticControl[K, M](_))
      }
    }

    case class MultiStatic[K](
        extractor: (In, Ctx, Set[K]) => Set[K],
        keys: Set[K])
        extends Multi[K] {
      override private[PartitionTreeBuilder] type MV[M] = PartitionControl.StaticControl[K, M]
      override def build[O, M](
          flowF: K => FlowWithExtendedContext[In, O, Ctx, M]
        ): FlowWithExtendedContext[In, FOut[O], Ctx, MV[M]] = {
        val passthroughFlow = FlowWithExtendedContext[In, Ctx].map(_ => None)
        val flowMap = keys.iterator.map(k => k -> flowF(k).map(Some(_))).toMap
        Partition
          .staticMulti(extractor, flowMap, passthroughFlow)
          .map(_.flatten)
          .mapMaterializedValue(new PartitionControl.StaticControl[K, M](_))
      }
    }
  }

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
  private[PartitionTreeBuilder] implicit val rootCanBuildOneForOneT: Layer.CanBuildOneForOneT[Lambda[M => M], Root] =
    new Layer.CanBuildOneForOneT[Lambda[M => M], Root] {
      def buildOneForOne[O, M](
          layer: Root,
          flowF: Root#KS => FlowWithExtendedContext[In, O, Ctx, M]
        ): FlowWithExtendedContext[In, O, Ctx, M] = flowF(KNil)
    }
  private[PartitionTreeBuilder] implicit val rootCanBuildOptionalT: Layer.CanBuildOptionalT[Lambda[M => M], Root] =
    new Layer.CanBuildOptionalT[Lambda[M => M], Root] {
      def buildOptional[O, M](
          layer: Root,
          flowF: Root#KS => FlowWithExtendedContext[In, Option[O], Ctx, M]
        ): FlowWithExtendedContext[In, Option[O], Ctx, M] = flowF(KNil)
    }
  private[PartitionTreeBuilder] implicit val rootCanBuildMultiT: Layer.CanBuildMultiT[Lambda[M => M], Root] =
    new Layer.CanBuildMultiT[Lambda[M => M], Root] {
      def buildMulti[O, M](
          layer: Root,
          flowF: Root#KS => FlowWithExtendedContext[In, immutable.Iterable[O], Ctx, M]
        ): FlowWithExtendedContext[In, immutable.Iterable[O], Ctx, M] = flowF(KNil)
    }

  sealed trait Root extends Layer[Lambda[M => M]] {
    override private[PartitionTreeBuilder] type FOut[O] = O
    override private[PartitionTreeBuilder] type KS = KNil

    def dynamicAuto[K](
        extractor: (In, Ctx) => K,
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): OneForOne[K, PartitioningProps.SingleDynamicAuto[K], Lambda[M => M], Root] = {
      val prop = PartitioningProps.SingleDynamicAuto(extractor, completionCriteria, bufferSize)
      OneForOne[K, PartitioningProps.SingleDynamicAuto[K], Lambda[M => M], Root](prop, Root)
    }

    def dynamicAutoMulticast[K](
        extractor: (In, Ctx, Set[K]) => Set[K],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Multi[K, PartitioningProps.MultiDynamicAuto[K], Lambda[M => M], Root] = {
      val prop = PartitioningProps.MultiDynamicAuto(extractor, completionCriteria, bufferSize)
      Multi[K, PartitioningProps.MultiDynamicAuto[K], Lambda[M => M], Root](prop, Root)
    }

    def dynamicManual[K](
        extractor: (In, Ctx) => K,
        initialKeys: Set[K],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Optional[K, PartitioningProps.SingleDynamicManual[K], Lambda[M => M], Root] = {
      val prop =
        PartitioningProps.SingleDynamicManual(
          extractor,
          initialKeys,
          completionCriteria,
          bufferSize
        )
      Optional[K, PartitioningProps.SingleDynamicManual[K], Lambda[M => M], Root](prop, Root)
    }

    def dynamicManualMulticast[K](
        extractor: (In, Ctx, Set[K]) => Set[K],
        initialKeys: Set[K],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Multi[K, PartitioningProps.MultiDynamicManual[K], Lambda[M => M], Root] = {
      val prop =
        PartitioningProps.MultiDynamicManual(extractor, initialKeys, completionCriteria, bufferSize)
      Multi[K, PartitioningProps.MultiDynamicManual[K], Lambda[M => M], Root](prop, Root)
    }

    def static[K](
        extractor: (In, Ctx) => K,
        keys: Set[K]
      ): OneForOne[K, PartitioningProps.SingleStatic[K], Lambda[M => M], Root] = {
      val prop = PartitioningProps.SingleStatic(extractor, keys)
      OneForOne[K, PartitioningProps.SingleStatic[K], Lambda[M => M], Root](prop, Root)
    }

    def staticMulticast[K](
        extractor: (In, Ctx, Set[K]) => Set[K],
        keys: Set[K]
      ): Multi[K, PartitioningProps.MultiStatic[K], Lambda[M => M], Root] = {
      val prop = PartitioningProps.MultiStatic(extractor, keys)
      Multi[K, PartitioningProps.MultiStatic[K], Lambda[M => M], Root](prop, Root)
    }
  }

  implicit private[PartitionTreeBuilder] def oneForOneCanBuildOneForOneT[
      K,
      Props <: PartitioningProps.OneForOne[K],
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
        val levelFlowF = (ks: Parent#KS) => layer.props.build[O, M](k => flowF(k :: ks))
        ev.buildOneForOne(layer.parent, levelFlowF)
      }
    }

  implicit private[PartitionTreeBuilder] def oneForOneCanBuildOptionalT[
      K,
      Props <: PartitioningProps.OneForOne[K],
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
        val levelFlowF = (ks: Parent#KS) => layer.props.build[Option[O], M](k => flowF(k :: ks))
        ev.buildOneForOne(layer.parent, levelFlowF)
      }
    }

  implicit private[PartitionTreeBuilder] def oneForOneCanBuildMultiT[
      K,
      Props <: PartitioningProps.OneForOne[K],
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
          layer.props.build[immutable.Iterable[O], M](k => flowF(k :: ks))
        ev.buildOneForOne(layer.parent, levelFlowF)
      }
    }

  case class OneForOne[
      K,
      Props <: PartitioningProps.OneForOne[K],
      ParentMV[_],
      Parent <: Layer[ParentMV]
    ](props: Props,
      parent: Parent
    )(implicit ev: Layer.CanBuildOneForOneT[ParentMV, Parent])
      extends Layer[Lambda[M => ParentMV[Props#MV[M]]]] {
    override private[PartitionTreeBuilder] type KS = K :: Parent#KS

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

    def dynamicAuto[K1](
        extractor: (In, Ctx) => K1,
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): OneForOne[K1, PartitioningProps.SingleDynamicAuto[K1], Lambda[M => ParentMV[Props#MV[M]]], OneForOne[K, Props, ParentMV, Parent]] = {
      val prop = PartitioningProps.SingleDynamicAuto(extractor, completionCriteria, bufferSize)
      OneForOne[K1, PartitioningProps.SingleDynamicAuto[K1], Lambda[M => ParentMV[Props#MV[M]]], OneForOne[K, Props, ParentMV, Parent]](prop, this)
    }

    def dynamicAutoMulticast[K1](
        extractor: (In, Ctx, Set[K1]) => Set[K1],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Multi[K1, PartitioningProps.MultiDynamicAuto[K1], Lambda[M => ParentMV[Props#MV[M]]], OneForOne[K, Props, ParentMV, Parent]] = {
      val prop = PartitioningProps.MultiDynamicAuto(extractor, completionCriteria, bufferSize)
      Multi[K1, PartitioningProps.MultiDynamicAuto[K1], Lambda[M => ParentMV[Props#MV[M]]], OneForOne[K, Props, ParentMV, Parent]](prop, this)
    }

    def dynamicManual[K1](
        extractor: (In, Ctx) => K1,
        initialKeys: Set[K1],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Optional[K1, PartitioningProps.SingleDynamicManual[K1], Lambda[M => ParentMV[Props#MV[M]]], OneForOne[K, Props, ParentMV, Parent]] = {
      val prop =
        PartitioningProps.SingleDynamicManual(
          extractor,
          initialKeys,
          completionCriteria,
          bufferSize
        )
      Optional[K1, PartitioningProps.SingleDynamicManual[K1], Lambda[M => ParentMV[Props#MV[M]]], OneForOne[K, Props, ParentMV, Parent]](prop, this)
    }

    def dynamicManualMulticast[K1](
        extractor: (In, Ctx, Set[K1]) => Set[K1],
        initialKeys: Set[K1],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Multi[K1, PartitioningProps.MultiDynamicManual[K1], Lambda[M => ParentMV[Props#MV[M]]], OneForOne[K, Props, ParentMV, Parent]] = {
      val prop =
        PartitioningProps.MultiDynamicManual(extractor, initialKeys, completionCriteria, bufferSize)
      Multi[K1, PartitioningProps.MultiDynamicManual[K1], Lambda[M => ParentMV[Props#MV[M]]], OneForOne[K, Props, ParentMV, Parent]](prop, this)
    }

    def static[K1](
        extractor: (In, Ctx) => K1,
        keys: Set[K1]
      ): OneForOne[K1, PartitioningProps.SingleStatic[K1], Lambda[M => ParentMV[Props#MV[M]]], OneForOne[K, Props, ParentMV, Parent]] = {
      val prop = PartitioningProps.SingleStatic(extractor, keys)
      OneForOne[K1, PartitioningProps.SingleStatic[K1], Lambda[M => ParentMV[Props#MV[M]]], OneForOne[K, Props, ParentMV, Parent]](prop, this)
    }

    def staticMulticast[K1](
        extractor: (In, Ctx, Set[K1]) => Set[K1],
        keys: Set[K1]
      ): Multi[K1, PartitioningProps.MultiStatic[K1], Lambda[M => ParentMV[Props#MV[M]]], OneForOne[K, Props, ParentMV, Parent]] = {
      val prop = PartitioningProps.MultiStatic(extractor, keys)
      Multi[K1, PartitioningProps.MultiStatic[K1], Lambda[M => ParentMV[Props#MV[M]]], OneForOne[K, Props, ParentMV, Parent]](prop, this)
    }
  }

  implicit private[PartitionTreeBuilder] def optionalCanBuildOptionalT[
      K,
      Props <: PartitioningProps.Optional[K],
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
          layer.props.build[Option[O], M](k => flowF(k :: ks)).map(_.flatten)
        ev.buildOptional(layer.parent, levelFlowF)
      }
    }

  implicit private[PartitionTreeBuilder] def optionalCanBuildMultiT[
      K,
      Props <: PartitioningProps.Optional[K],
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
          layer.props.build[immutable.Iterable[O], M](k => flowF(k :: ks))
        ev
          .buildOptional(layer.parent, levelFlowF)
          .map[immutable.Iterable[O]](_.toList.flatten)
          .asInstanceOf[FlowWithExtendedContext[In, immutable.Iterable[O], Ctx, ParentMV[
            Props#MV[M]
          ]]]
      }
    }

  case class Optional[
      K,
      Props <: PartitioningProps.Optional[K],
      ParentMV[_],
      Parent <: Layer[ParentMV]
    ](props: Props,
      parent: Parent
    )(implicit ev: Layer.CanBuildOptionalT[ParentMV, Parent])
      extends Layer[Lambda[M => ParentMV[Props#MV[M]]]] {
    override private[PartitionTreeBuilder] type KS = K :: Parent#KS

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

    def dynamicAuto[K1](
        extractor: (In, Ctx) => K1,
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Optional[K1, PartitioningProps.OneForOneAsOptional[K1, PartitioningProps.SingleDynamicAuto[K1]], Lambda[M => ParentMV[Props#MV[M]]], Optional[K, Props, ParentMV, Parent]] = {
      val baseProp =
        PartitioningProps.SingleDynamicAuto[K1](extractor, completionCriteria, bufferSize)
      val prop =
        PartitioningProps.OneForOneAsOptional[K1, PartitioningProps.SingleDynamicAuto[K1]](baseProp)
      Optional[K1, PartitioningProps.OneForOneAsOptional[K1, PartitioningProps.SingleDynamicAuto[K1]], Lambda[M => ParentMV[Props#MV[M]]], Optional[K, Props, ParentMV, Parent]](prop, this)
    }

    def dynamicAutoMulticast[K1](
        extractor: (In, Ctx, Set[K1]) => Set[K1],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Multi[K1, PartitioningProps.MultiDynamicAuto[K1], Lambda[M => ParentMV[Props#MV[M]]], Optional[K, Props, ParentMV, Parent]] = {
      val prop = PartitioningProps.MultiDynamicAuto(extractor, completionCriteria, bufferSize)
      Multi[K1, PartitioningProps.MultiDynamicAuto[K1], Lambda[M => ParentMV[Props#MV[M]]], Optional[K, Props, ParentMV, Parent]](prop, this)
    }

    def dynamicManual[K1](
        extractor: (In, Ctx) => K1,
        initialKeys: Set[K1],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Optional[K1, PartitioningProps.SingleDynamicManual[K1], Lambda[M => ParentMV[Props#MV[M]]], Optional[K, Props, ParentMV, Parent]] = {
      val prop =
        PartitioningProps.SingleDynamicManual(
          extractor,
          initialKeys,
          completionCriteria,
          bufferSize
        )
      Optional[K1, PartitioningProps.SingleDynamicManual[K1], Lambda[M => ParentMV[Props#MV[M]]], Optional[K, Props, ParentMV, Parent]](prop, this)
    }

    def dynamicManualMulticast[K1](
        extractor: (In, Ctx, Set[K1]) => Set[K1],
        initialKeys: Set[K1],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Multi[K1, PartitioningProps.MultiDynamicManual[K1], Lambda[M => ParentMV[Props#MV[M]]], Optional[K, Props, ParentMV, Parent]] = {
      val prop =
        PartitioningProps.MultiDynamicManual(extractor, initialKeys, completionCriteria, bufferSize)
      Multi[K1, PartitioningProps.MultiDynamicManual[K1], Lambda[M => ParentMV[Props#MV[M]]], Optional[K, Props, ParentMV, Parent]](prop, this)
    }

    def static[K1](
        extractor: (In, Ctx) => K1,
        keys: Set[K1]
      ): Optional[K1, PartitioningProps.OneForOneAsOptional[K1, PartitioningProps.SingleStatic[K1]], Lambda[M => ParentMV[Props#MV[M]]], Optional[K, Props, ParentMV, Parent]] = {
      val baseProp = PartitioningProps.SingleStatic(extractor, keys)
      val prop =
        PartitioningProps.OneForOneAsOptional[K1, PartitioningProps.SingleStatic[K1]](baseProp)
      Optional[K1, PartitioningProps.OneForOneAsOptional[K1, PartitioningProps.SingleStatic[K1]], Lambda[M => ParentMV[Props#MV[M]]], Optional[K, Props, ParentMV, Parent]](prop, this)
    }

    def staticMulticast[K1](
        extractor: (In, Ctx, Set[K1]) => Set[K1],
        keys: Set[K1]
      ): Multi[K1, PartitioningProps.MultiStatic[K1], Lambda[M => ParentMV[Props#MV[M]]], Optional[K, Props, ParentMV, Parent]] = {
      val prop = PartitioningProps.MultiStatic(extractor, keys)
      Multi[K1, PartitioningProps.MultiStatic[K1], Lambda[M => ParentMV[Props#MV[M]]], Optional[K, Props, ParentMV, Parent]](prop, this)
    }
  }

  implicit private[PartitionTreeBuilder] def multiCanBuildMultiT[
      K,
      Props <: PartitioningProps.Multi[K],
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
          layer.props.build[immutable.Iterable[O], M](k => flowF(k :: ks)).map(_.flatten)
        ev.buildMulti(layer.parent, levelFlowF)
      }
    }

  case class Multi[K, Props <: PartitioningProps.Multi[K], ParentMV[_], Parent <: Layer[ParentMV]](
      props: Props,
      parent: Parent
    )(implicit ev: Layer.CanBuildMultiT[ParentMV, Parent])
      extends Layer[Lambda[M => ParentMV[Props#MV[M]]]] {
    override private[PartitionTreeBuilder] type KS = K :: Parent#KS

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

    def dynamicAuto[K1](
        extractor: (In, Ctx) => K1,
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Multi[K1, PartitioningProps.OneForOneAsMulti[K1, PartitioningProps.SingleDynamicAuto[K1]], Lambda[M => ParentMV[Props#MV[M]]], Multi[K, Props, ParentMV, Parent]] = {
      val baseProp =
        PartitioningProps.SingleDynamicAuto[K1](extractor, completionCriteria, bufferSize)
      val prop =
        PartitioningProps.OneForOneAsMulti[K1, PartitioningProps.SingleDynamicAuto[K1]](baseProp)
      Multi[K1, PartitioningProps.OneForOneAsMulti[K1, PartitioningProps.SingleDynamicAuto[K1]], Lambda[M => ParentMV[Props#MV[M]]], Multi[K, Props, ParentMV, Parent]](prop, this)
    }

    def dynamicAutoMulticast[K1](
        extractor: (In, Ctx, Set[K1]) => Set[K1],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Multi[K1, PartitioningProps.MultiDynamicAuto[K1], Lambda[M => ParentMV[Props#MV[M]]], Multi[K, Props, ParentMV, Parent]] = {
      val prop = PartitioningProps.MultiDynamicAuto(extractor, completionCriteria, bufferSize)
      Multi[K1, PartitioningProps.MultiDynamicAuto[K1], Lambda[M => ParentMV[Props#MV[M]]], Multi[K, Props, ParentMV, Parent]](prop, this)
    }

    def dynamicManual[K1](
        extractor: (In, Ctx) => K1,
        initialKeys: Set[K1],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Multi[K1, PartitioningProps.OptionalAsMulti[K1, PartitioningProps.SingleDynamicManual[K1]], Lambda[M => ParentMV[Props#MV[M]]], Multi[K, Props, ParentMV, Parent]] = {
      val baseProp =
        PartitioningProps.SingleDynamicManual(
          extractor,
          initialKeys,
          completionCriteria,
          bufferSize
        )
      val prop =
        PartitioningProps.OptionalAsMulti[K1, PartitioningProps.SingleDynamicManual[K1]](baseProp)
      Multi[K1, PartitioningProps.OptionalAsMulti[K1, PartitioningProps.SingleDynamicManual[K1]], Lambda[M => ParentMV[Props#MV[M]]], Multi[K, Props, ParentMV, Parent]](prop, this)
    }

    def dynamicManualMulticast[K1](
        extractor: (In, Ctx, Set[K1]) => Set[K1],
        initialKeys: Set[K1],
        completionCriteria: PartitionDynamic.CompletionCriteria[In, Any, Ctx] =
          PartitionDynamic.defaultCompletionCriteria,
        bufferSize: Int = PartitionDynamic.defaultBufferSize
      ): Multi[K1, PartitioningProps.MultiDynamicManual[K1], Lambda[M => ParentMV[Props#MV[M]]], Multi[K, Props, ParentMV, Parent]] = {
      val prop =
        PartitioningProps.MultiDynamicManual(extractor, initialKeys, completionCriteria, bufferSize)
      Multi[K1, PartitioningProps.MultiDynamicManual[K1], Lambda[M => ParentMV[Props#MV[M]]], Multi[K, Props, ParentMV, Parent]](prop, this)
    }

    def static[K1](
        extractor: (In, Ctx) => K1,
        keys: Set[K1]
      ): Multi[K1, PartitioningProps.OneForOneAsMulti[K1, PartitioningProps.SingleStatic[K1]], Lambda[M => ParentMV[Props#MV[M]]], Multi[K, Props, ParentMV, Parent]] = {
      val baseProp = PartitioningProps.SingleStatic(extractor, keys)
      val prop =
        PartitioningProps.OneForOneAsMulti[K1, PartitioningProps.SingleStatic[K1]](baseProp)
      Multi[K1, PartitioningProps.OneForOneAsMulti[K1, PartitioningProps.SingleStatic[K1]], Lambda[M => ParentMV[Props#MV[M]]], Multi[K, Props, ParentMV, Parent]](prop, this)
    }

    def staticMulticast[K1](
        extractor: (In, Ctx, Set[K1]) => Set[K1],
        keys: Set[K1]
      ): Multi[K1, PartitioningProps.MultiStatic[K1], Lambda[M => ParentMV[Props#MV[M]]], Multi[K, Props, ParentMV, Parent]] = {
      val prop = PartitioningProps.MultiStatic(extractor, keys)
      Multi[K1, PartitioningProps.MultiStatic[K1], Lambda[M => ParentMV[Props#MV[M]]], Multi[K, Props, ParentMV, Parent]](prop, this)
    }
  }
}
