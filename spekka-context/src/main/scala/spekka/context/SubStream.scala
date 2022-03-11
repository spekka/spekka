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
import akka.NotUsed
import akka.stream.Attributes
import akka.stream.Inlet
import akka.stream.Outlet
import akka.stream.SinkShape
import akka.stream.SourceShape
import akka.stream.scaladsl.Sink
import akka.stream.scaladsl.Source
import akka.stream.stage.AsyncCallback
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.GraphStageWithMaterializedValue
import akka.stream.stage.InHandler
import akka.stream.stage.OutHandler

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.Promise

/** Mixin class for `GraphStageLogic` providing sub stream support.
  *
  * The sub stream functionality is modeled in a similar way to Akka with the goal of being usable
  * by user code.
  *
  * SubStreams are formed by connecting a [[SubStream.SubOutlet]] to a [[SubStream.SubInlet]] and
  * can be used to dynamically partition data flow inside a stage without creating async barriers
  * (i.e. computation happen inside the main stage thread).
  */
trait SubStream { self: GraphStageLogic =>

  /** Creates a source linked to the returned [[SubStream.SubOutlet]].
    *
    * Data pushed to the outlet will be streamed to the source.
    *
    * @param buffer
    *   The buffer used by the substream
    * @param handler
    *   The SubOutlet.SubOutHandler backing the source
    * @return
    *   Sub outlet with associated source
    */
  protected def getSubSource[T](
      buffer: Int,
      handler: SubStream.SubOutHandler[T]
    ): (SubStream.SubOutlet[T], Source[T, NotUsed]) = {
    val (subOutlet, refs) = getSubOutlet[T](handler)
    val src = Source.fromGraph(new SubStream.SubSource[T](refs, buffer))

    subOutlet -> src
  }

  /** Creates a sink connected to the returned [[SubStream.SubInlet]].
    *
    * Data streamed to the sink will be pushed to the inlet.
    *
    * @param buffer
    *   The buffer used by the substream
    * @param handler
    *   The SubOutlet.SubInHandler backing the sink
    * @return
    *   Sub inlet with associated sink
    */
  protected def getSubSink[T](
      buffer: Int,
      handler: SubStream.SubInHandler[T]
    ): (SubStream.SubInlet[T], Sink[T, Future[Done]]) = {
    val (subInlet, refs) = getSubInlet[T](handler, buffer)

    val sink = Sink.fromGraph(new SubStream.SubSink[T](refs))

    subInlet -> sink
  }

  private[SubStream] def getSubOutlet[T](
      handler: SubStream.SubOutHandler[T]
    ): (SubStream.SubOutlet[T], SubStream.SubOutlet.Refs[T]) = {
    val outlet = new SubStream.SubOutlet[T](handler)

    val initializeCallback = getAsyncCallback[SubStream.SubInlet.Refs[T]] { init =>
      outlet.initialize(
        init.pushCallback,
        init.failureCallback
      )
    }

    val pullCallback = getAsyncCallback[Int] { request =>
      outlet.onPullImpl(request)
    }

    val cancelCallback = getAsyncCallback[Throwable] { cause =>
      outlet.onDownstreamFinishImpl(cause)
    }

    val refs = SubStream.SubOutlet.Refs(
      initializeCallback,
      pullCallback,
      cancelCallback
    )

    outlet -> refs
  }

  private[SubStream] def getSubInlet[T](
      handler: SubStream.SubInHandler[T],
      buffer: Int
    ): (SubStream.SubInlet[T], SubStream.SubInlet.Refs[T]) = {
    val inlet = new SubStream.SubInlet[T](handler, buffer)

    val initializeCallback = getAsyncCallback[SubStream.SubOutlet.Refs[T]] { init =>
      inlet.initialize(init.pullCallback, init.cancelCallback)
    }

    val pushCallback = getAsyncCallback[T] { element =>
      inlet.onPushImpl(element)
    }

    val failureCallback = getAsyncCallback[Throwable] { cause =>
      inlet.onFailureImpl(cause)
    }

    val refs = SubStream.SubInlet.Refs(
      initializeCallback,
      pushCallback,
      failureCallback
    )

    inlet -> refs
  }

  private[SubStream] def connect[T](
      outletRefs: SubStream.SubOutlet.Refs[T],
      buffer: Int
    )(handler: SubStream.SubInHandler[T]
    ): SubStream.SubInlet[T] = {
    val inlet = new SubStream.SubInlet[T](handler, buffer)

    inlet.initialize(outletRefs.pullCallback, outletRefs.cancelCallback)

    val pushCallback = getAsyncCallback[T] { element =>
      inlet.onPushImpl(element)
    }

    val failureCallback = getAsyncCallback[Throwable] { cause =>
      inlet.onFailureImpl(cause)
    }

    val refs = SubStream.SubInlet.Refs[T](
      null,
      pushCallback,
      failureCallback
    )

    outletRefs.initializeCallback.invoke(refs)

    inlet
  }

  private[SubStream] def connect[T](
      inletRefs: SubStream.SubInlet.Refs[T]
    )(handler: SubStream.SubOutHandler[T]
    ): SubStream.SubOutlet[T] = {
    val outlet = new SubStream.SubOutlet[T](handler)

    outlet.initialize(inletRefs.pushCallback, inletRefs.failureCallback)

    val pullCallback = getAsyncCallback[Int] { request =>
      outlet.onPullImpl(request)
    }

    val cancelCallback = getAsyncCallback[Throwable] { cause =>
      outlet.onDownstreamFinishImpl(cause)
    }

    val refs = SubStream.SubOutlet.Refs[T](null, pullCallback, cancelCallback)
    inletRefs.initializeCallback.invoke(refs)

    outlet
  }
}

object SubStream {

  /** Sub inlet handler
    */
  trait SubInHandler[T] {

    /** Notifies that the sub stream associated to the provided [[SubStream.SubInlet]] has been
      * correctly initialized and it is ready to be used.
      *
      * @param subInlet
      *   the sub inlet associated to the initialized sub stream
      */
    def initialized(subInlet: SubInlet[T]): Unit

    /** Callback invoked when data is pushed to the sub stream associated to the provided
      * [[SubStream.SubInlet]].
      *
      * @param subInlet
      *   the sub inlet associated to the initialized sub stream
      * @param element
      *   the pushed element
      */
    def onPush(subInlet: SubInlet[T], element: T): Unit

    /** Callback invoked when the sub stream associated to the provided [[SubStream.SubInlet]] is
      * completed.
      *
      * @param subInlet
      *   the sub inlet associated to the initialized sub stream
      */
    def onUpstreamFinish(subInlet: SubInlet[T]): Unit

    /** Callback invoked when the sub stream associated to the provided [[SubStream.SubInlet]]
      * fails.
      * @param subInlet
      *   the sub inlet associated to the initialized sub stream
      * @param cause
      *   the failure cause
      */
    def onUpstreamFailure(subInlet: SubInlet[T], cause: Throwable): Unit
  }

  object SubInlet {
    private[SubStream] case class Refs[T](
        initializeCallback: AsyncCallback[SubOutlet.Refs[T]],
        pushCallback: AsyncCallback[T],
        failureCallback: AsyncCallback[Throwable])
  }

  class SubInlet[T](handler: SubInHandler[T], bufferSize: Int) {
    require(bufferSize >= 1, "Buffer size must be grater or equal to 1")
    private var initialized: Boolean = false
    private var deferredFailure: Throwable = null
    private var pulled: Boolean = false
    private var requested: Int = 0
    private var completed: Boolean = false
    private var deferredCompletion: Boolean = false
    private var failed: Boolean = false
    private val buffer: mutable.ListBuffer[T] = mutable.ListBuffer()

    private var pullCallback: AsyncCallback[Int] = null
    private var cancelCallback: AsyncCallback[Throwable] = null

    final private[spekka] def initialize(
        _pullCallback: AsyncCallback[Int],
        _cancelCallback: AsyncCallback[Throwable]
      ): Unit = {
      if (initialized)
        throw new IllegalStateException("Cannot initialize an already initialized subinlet!")
      initialized = true
      pullCallback = _pullCallback
      cancelCallback = _cancelCallback

      handler.initialized(this)

      if (failed) cancelCallback.invoke(deferredFailure)
    }

    def isInitialized: Boolean = initialized

    def isClosed: Boolean = completed || failed

    private def pullSubOutletIfNeeded(): Unit = {
      if (requested <= bufferSize / 2) {
        val diff = bufferSize - requested
        requested = bufferSize
        pullCallback.invoke(diff)
      }
    }

    final private[spekka] def onPushImpl(element: T): Unit = {
      if (!isClosed) {
        if (requested == 0) throw new IllegalStateException("Received unrequested push!")
        requested -= 1
        if (element == null) {
          completed = true
          if (buffer.isEmpty) handler.onUpstreamFinish(this)
          else deferredCompletion = true
        } else {
          if (pulled) {
            pulled = false
            handler.onPush(this, element)
          } else buffer += element
          pullSubOutletIfNeeded()
        }
      }
    }

    final private[spekka] def onFailureImpl(cause: Throwable): Unit = {
      if (!isClosed) {
        failed = true
        handler.onUpstreamFailure(this, cause)
      }
    }

    def hasBeenPulled(): Boolean = {
      pulled
    }

    def pull(): Unit = {
      if (!initialized) throw new IllegalStateException("Cannot pull an uninitialized subinlet!")
      else if (isClosed && !deferredCompletion)
        throw new IllegalStateException("Cannot pull an already closed subinlet!")

      if (buffer.nonEmpty) handler.onPush(this, buffer.remove(0))
      else if (completed) {
        deferredCompletion = false
        handler.onUpstreamFinish(this)
      } else if (!pulled) {
        pullSubOutletIfNeeded()
        pulled = true
      } else throw new IllegalStateException("Cannot pull already pulled subinlet!")
    }

    def cancel(cause: Throwable): Unit = {
      if (!initialized) throw new IllegalStateException("Cannot cancel an uninitialized subinlet!")
      else if (isClosed)
        throw new IllegalStateException("Cannot cancel an already closed subinlet!")

      failed = true
      if (initialized) cancelCallback.invoke(cause)
      else deferredFailure = cause
    }
  }

  /** Sub outlet handler.
    */
  trait SubOutHandler[T] {

    /** Notifies that the sub stream associated to the provided [[SubStream.SubOutlet]] has been
      * correctly initialized and it is ready to be used.
      *
      * @param subOutlet
      *   the sub outlet associated to the initialized sub stream
      */
    def initialized(subOutlet: SubOutlet[T]): Unit

    /** Callback invoked when data is requested by the sub stream associated to the provided
      * [[SubStream.SubOutlet]].
      *
      * @param subOutlet
      *   the sub outlet associated to the sub stream demanding data
      */
    def onPull(subOutlet: SubOutlet[T]): Unit

    /** Callback invoked when the source linked to the provided [[SubStream.SubOutlet]] is
      * cancelled.
      *
      * @param subOutlet
      *   the sub outlet associated to the cancelled stream
      * @param cause
      *   Cancellation cause
      */
    def onDownstreamFinish(subOutlet: SubOutlet[T], cause: Throwable): Unit
  }

  object SubOutlet {
    private[SubStream] case class Refs[T](
        initializeCallback: AsyncCallback[SubInlet.Refs[T]],
        pullCallback: AsyncCallback[Int],
        cancelCallback: AsyncCallback[Throwable])
  }

  class SubOutlet[T](handler: SubOutHandler[T]) {
    private var initialized: Boolean = false
    private var deferredPull: Option[Int] = None
    private var deferredFailure: Throwable = null

    private var pulled: Boolean = false
    private var requested: Int = 0
    private var completed: Boolean = false
    private var failed: Boolean = false

    private val buffer: mutable.ListBuffer[T] = mutable.ListBuffer()

    private var pushCallback: AsyncCallback[T] = null
    private var failureCallback: AsyncCallback[Throwable] = null

    final private[spekka] def initialize(
        _pushCallback: AsyncCallback[T],
        _failureCallback: AsyncCallback[Throwable]
      ): Unit = {
      if (initialized)
        throw new IllegalStateException("Cannot initialize an already initialized suboutlet!")

      initialized = true
      pushCallback = _pushCallback
      failureCallback = _failureCallback
      handler.initialized(this)

      if (failed) {
        failureCallback.invoke(deferredFailure)
      } else {
        deferredPull.foreach(onPullImpl)
        deferredPull = None
      }
    }

    def isInitialized: Boolean = initialized

    def isClosed: Boolean = completed || failed

    def isAvailable: Boolean = initialized && requested > 0

    final private[spekka] def onPullImpl(request: Int): Unit = {
      if (initialized) {
        requested += request

        if (buffer.nonEmpty) {
          val n = Math.min(requested, buffer.size)
          buffer.iterator.take(n).foreach(pushCallback.invoke)
          buffer.remove(0, n)
          requested -= n
        } else if (!isClosed) {
          if (!pulled) {
            pulled = true
            handler.onPull(this)
          }
        } else if (completed) {
          pushCallback.invoke(null.asInstanceOf[T])
        }
      } else {
        if (deferredPull.isEmpty) deferredPull = Some(request)
        else throw new IllegalStateException("Received multiple pull while initializing!")
      }
    }

    final private[spekka] def onDownstreamFinishImpl(cause: Throwable): Unit = {
      if (!isClosed) {
        failed = true
        handler.onDownstreamFinish(this, cause)
      }
    }

    def push(element: T): Unit = {
      if (!initialized) throw new IllegalStateException("Cannot push an uninitialized suboutlet!")
      else if (isClosed) throw new IllegalStateException("Cannot push an already closed suboutlet!")

      if (requested > 0) {
        requested -= 1
        pushCallback.invoke(element)

        if (requested > 0) {
          handler.onPull(this)
        } else pulled = false
      } else throw new IllegalStateException("Cannot push suboutlet until pulled!")
    }

    def pushOrBuffer(element: T): Unit = {
      if (isClosed) throw new IllegalStateException("Cannot push an already closed suboutlet!")

      if (buffer.nonEmpty) buffer += element
      else if (isAvailable) push(element)
      else buffer += element
    }

    def complete(): Unit = {
      if (isClosed)
        throw new IllegalStateException("Cannot complete an already closed suboutlet!")

      completed = true
      if (isInitialized && buffer.isEmpty && requested > 0) {
        pushCallback.invoke(null.asInstanceOf[T])
      }
    }

    def fail(cause: Throwable): Unit = {
      if (isClosed) throw new IllegalStateException("Cannot fail an already closed suboutlet!")
      failed = true

      if (!initialized) deferredFailure = cause
      else failureCallback.invoke(cause)
    }
  }

  class SubSource[T](outRef: SubOutlet.Refs[T], buffer: Int) extends GraphStage[SourceShape[T]] {
    val out: Outlet[T] = Outlet("SubSource.out")
    override def shape: SourceShape[T] = SourceShape(out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) with SubStream {
        val subIn = connect(outRef, buffer)(new SubInHandler[T] {
          override def initialized(subInlet: SubInlet[T]): Unit = {}

          override def onPush(subInlet: SubInlet[T], element: T): Unit = push(out, element)

          override def onUpstreamFinish(subInlet: SubInlet[T]): Unit = {
            completeStage()
          }

          override def onUpstreamFailure(subInlet: SubInlet[T], cause: Throwable): Unit =
            failStage(cause)
        })

        setHandler(
          out,
          new OutHandler {
            override def onPull(): Unit = {
              subIn.pull()
            }

            override def onDownstreamFinish(cause: Throwable): Unit = {
              subIn.cancel(cause)
            }
          }
        )
      }
  }

  class SubSink[T](inRef: SubInlet.Refs[T])
      extends GraphStageWithMaterializedValue[SinkShape[T], Future[Done]] {
    val in: Inlet[T] = Inlet("SubSink.in")
    override def shape: SinkShape[T] = SinkShape(in)

    override def createLogicAndMaterializedValue(
        inheritedAttributes: Attributes
      ): (GraphStageLogic, Future[Done]) = {
      val promise = Promise[Done]()
      val logic = new GraphStageLogic(shape) with SubStream {

        val subOut = connect(inRef)(
          new SubOutHandler[T] {
            override def initialized(subOutlet: SubOutlet[T]): Unit = {}

            override def onPull(subOutlet: SubOutlet[T]): Unit = {
              if (isClosed(in)) {
                subOutlet.complete()
                promise.trySuccess(Done)
                setKeepGoing(false)
                completeStage()
              } else pull(in)

            }

            override def onDownstreamFinish(subOutlet: SubOutlet[T], cause: Throwable): Unit =
              cancel(in, cause)
          }
        )

        setHandler(
          in,
          new InHandler {
            override def onPush(): Unit = {
              subOut.push(grab(in))
            }

            override def onUpstreamFinish(): Unit = {
              // If suboutlet didn't pull yet, defer the completion
              if (subOut.isAvailable) {
                subOut.complete()
                promise.trySuccess(Done)
                ()
              } else setKeepGoing(true)
            }

            override def onUpstreamFailure(cause: Throwable): Unit = {
              subOut.fail(cause)
              promise.tryFailure(cause)
              super.onUpstreamFailure(cause)
            }
          }
        )
      }

      logic -> promise.future
    }
  }
}
