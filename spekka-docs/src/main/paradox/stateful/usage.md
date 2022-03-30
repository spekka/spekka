## Stateful Flow Usage

Once we have generated `StatefulFlowProp` by combining a logic and a backend we are ready to register the flow for usage.

In order to do so we first need to create a `StatefulFlowRegistry` which will handle all the low level tasks needed for coordinating the stateful flows.

A registry can be created invoking `StatefulFlowRegistry.apply` specifying a timeout to use for communication with the registry itself and possibly a name for the registry (only useful if more than one registry instance is needed, which is not recommended).

@@snip[Registry.scala](/spekka-docs/src/main/scala/StatefulFlowEventBasedExample.scala) { #registry }

We can now register the flows (see the section on either @ref[Event Based](event-based.md) or @ref[Durable State](durable-state.md) to see how the flow was defined):

@@snip[Registration.scala](/spekka-docs/src/main/scala/StatefulFlowEventBasedExample.scala) { #registration }

At this point the `StatefulFlowBuilder` can be used to instantiate flows by specifying a particular *entity*:

@@snip[Instantiation.scala](/spekka-docs/src/main/scala/StatefulFlowEventBasedExample.scala) { #instantiation }

Once a stateful stream is running we can send command to it by using its `StatefulFlowControl` object (obtained either by accessing the flow materialized value or via the `StatefulFlowBuilder.control` method):

@@snip[Instantiation.scala](/spekka-docs/src/main/scala/StatefulFlowEventBasedExample.scala) { #query }
