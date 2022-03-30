# Event Based Stateful Flows

Event based stateful flows are defined in terms of *events* generated upon receiving a stream element or command.

They are well suited to model entity where the frequency of state changes is high (possibly with small deltas). Furthermore they are a natural fit for *event sourced* solutions.


## Logic

A `StatefulFlowLogic.EventBased[State, Ev, In, Command]` is a kind of logic which treats the state as an events log: inputs/commands generate events which are persisted and used to update the state.

The output type of an event based stateful flow is its event type, this means that a flow using an event based logic will produce as output the events generated while processing the inputs.

In order to adapt the counter example to use an event based stateful flow we need to start by defining the *state*, *event* and *command* models:

@@snip[Definitions.scala](/spekka-docs/src/main/scala/StatefulFlowEventBasedExample.scala) { #definitions }

Now that we have our models we can instantiate a logic operating on it:

@@snip[Logic.scala](/spekka-docs/src/main/scala/StatefulFlowEventBasedExample.scala) { #logic }

We do that by invoking `StatefulFlowLogic.EventBased` specifying the following 4 parameters:

1. **initial state:** the state value to use for the first instantiation of the flow (i.e. when no state can be recovered by the backend)
2. **input handler:** the function used to handle stream inputs by generating events (possibly inspecting the current state)
3. **state update function:** the function used to update the state by applying a single event
4. **command handler:** the function used to handle commands by generating events (possibly inspecting the current state)

The 2 handler functions for *inputs* and *commands* expects a result of type `StatefulFlowLogic.EventBased.ProcessingResult[Ev]`. This types serves as a representation of:

1. The *events* the input/command generates
2. The side effect we want to be performed **before** the state is updated with generated *events*
3. The side effect we want to be performed **after** the state has been successfully updated with the generated *events*

Thanks to the native side effect support it becomes easy to write pipelines of micro-services with *at-least-once* semantic (use Kafka as a backbone, read from `topicUpstream` and emit to `topicDownstream` inside a **before** side effect. If the process fails while emitting, we have a guarantee that upon restart the stream will resume processing where it left off, hence repeating the side effects.)

Similarly you can use **after** side effects to model *at-most-once* scenarios (if the system fails **after** the state has been modified, there is no guarantee that the logic will produce the same side effects upon restart).

***note
Both **before** and **after** side effects are evaluated as part of the stream. This means that
the flow will not produce any output until both side effects groups have been completed successfully. Similarly if any side effect fails (i.e. `Future.failed`) the stream will be failed as well.
***

## Backend

A `StatefulFlowBackend.EventBased[State, Ev, _]` is a kind of backend compatible with event based logics with the same *state* and *event* types.

Spekka Stateful ships with an in-memory implementation useful for testing and quick prototyping: `InMemoryStatefulFlowBackend.EventBased[State, Ev]`.

We can create a backend for our example with the following code:

@@snip[Backend.scala](/spekka-docs/src/main/scala/StatefulFlowEventBasedExample.scala) { #backend }

## Usage

Now that we have defined both the logic and the backend, we can obtain a `StatefulFlowProps` object describing our flow:

@@snip[Props.scala](/spekka-docs/src/main/scala/StatefulFlowEventBasedExample.scala) { #props }

We now register the flow for the 2 different usages we have planned: *counters by deployment* and *counters by entrances*:

@@snip[Registration.scala](/spekka-docs/src/main/scala/StatefulFlowEventBasedExample.scala) { #registration }

Now that we have registered the flows, we can use the corresponding `StatefulFlowBuilder` to instantiate a flow for a specific *entity*:

@@snip[Registration.scala](/spekka-docs/src/main/scala/StatefulFlowEventBasedExample.scala) { #instantiation }

You can find the full example here: @github[StatefulFlowEventBasedExample.scala](/spekka-docs/src/main/scala/StatefulFlowEventBasedExample.scala).