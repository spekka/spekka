# Flow With Extended Context

A `FlowWithExtendedContext[In, Out, Ctx, M]` is much like an Akka's `FlowWithContext` where the context type `Ctx` is wrapped in an `ExtendedContext`. The allowed operations on a `FlowWithExtendedContext` are limited in order to avoid:

- filtering operation (like `filter`, `filterNot`, `collect`)
- *one-to-n* (like `mapConcat`)

@@@ warning
It comes a point were such restrictions may make it impossible to implement a particular feature.
For this reason there is the possibility to convert a `FlowWithExtendedContext` to a regular flow (using `toGraph`) and back 
(using `FlowWithExtendedContext.fromGraphUnsafe`).

This method has an intentionally *scary* name as, when using it, the programmer is responsible of making sure that the
`FlowWithExtendedContext` guarantees have been respected. Failure to do so may result in stream deadlock and/or errors at run-time.
@@@

# Creating

A `FlowWithExtendedContext` can be created similarly to Akka `Flow` and `FlowWithContext`.

For instance, considering the following definition modeling a people counting system:

@@snip[Definitions.scala](/spekka-docs/src/main/scala/PeopleEntranceCounterModel.scala) { #definitions }

we can create a simple flow which computes the total number of people which entered a store with the following code:

@@snip[FlowDefinition.scala](/spekka-docs/src/main/scala/FlowWithExtendedContextBasicExample.scala) { #flow-definition}

# Connecting

Once a `FlowWithExtendedContext` instance has been created, it can be connected to other flows using the standard Akka's `via` and `viaMat` operators:

@@snip[FlowComposition.scala](/spekka-docs/src/main/scala/FlowWithExtendedContextBasicExample.scala) { #flow-composition}

What we achieved in this example is a stream which sums all the samples it receives as input (working under the assumption that they are coming from a single deployment) and *commits* the message offsets as soon as the processing is completed.

You can find the full example here: @github[FlowWithExtendedContextBasicExample.scala](/spekka-docs/src/main/scala/FlowWithExtendedContextBasicExample.scala).