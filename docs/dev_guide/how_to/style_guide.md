---
layout: default
title: Style Guide
parent: How to Contribute to Venice
grand_parent: Developer Guides
permalink: /docs/dev_guide/how_to/style_guide
---

# Style Guide

This page describes some stylistic concerns which the development team cares about. Some of them are enforced by 
automation, while others are guidelines of a more informal, or even philosophical, nature. More generally, we believe in 
acquiring a deep understanding of the principles behind these guidelines, and being thoughtful about which situation 
they apply or don't apply to. We don't buy into mainstream ideas such as "all coupling is bad", "all optimizations are
premature", etc. We take this common wisdom and incorporate it into our reflections, without blindly taking it at face
value.

We also want to acknowledge that our conclusions change over time, either due to hindsight or because of the evolving 
context that the project needs to navigate. As such, this should be thought of as a living document, and it is therefore 
natural that not all parts of the code base adhere to it perfectly. When such deviations are discovered, it is 
encouraged to try to rectify them "in passing", even if the objective of the code change is unrelated. This, too, is a 
judgment call, and needs to be balanced by concerns like managing the risk of unintended regressions.

## Automation

Regarding code style, we use the Eclipse Java Formatter variant of Spotless, which automatically reformats the code as
part of git commit hooks. Make sure to run `./gradlew assemble` to get the git hooks set up.

We also use Spotbugs, with [some of the rules](https://github.com/linkedin/venice/blob/main/gradle/spotbugs/include.xml) 
resulting in build failures if violated. Over time, we intend to pick up more of these rules, fix the code to comply 
with them, and add them to the list. If you would like to contribute to this effort, feel free to open an issue and 
suggest which rules you are interested in fixing. Note that there are [a few rules that we intend to ignore](https://github.com/linkedin/venice/blob/main/gradle/spotbugs/exclude.xml) 
as they seem to be imprecise or not sufficiently useful.

In the future, we might add other forms of automation to increase code quality along other dimensions. Feel free to
suggest ideas in this space.

## Guidelines

Below are a set of guidelines to take into consideration when developing in the Venice project. They are not absolute
rules and there may be good reasons to deviate from them occasionally. When deviating, it is useful to leave comments
explaining why we deviated, whether it was intentional, or due to the need for expediency. This helps future maintainers 
understand what is actually worth cleaning up and how careful they need to be when doing it.

### Compatibility

We care about compatibility across versions of the software. This is a bidirectional statement. Old clients need to be
able to talk with new servers, and new clients with old servers as well. It also includes interactions across lifetimes
of the same process, for example, state persisted by an old version of the server code should be usable by a newer 
version of the server code, and vice versa. If compatibility is impossible, then we should look for ways to achieve
correct behavior anyway (e.g. potentially at the cost of efficiency, such as the server needing to throw away and
regenerate the state).

For enums which are going to be communicated across processes or across lifetimes of the same process, consider using 
[VeniceEnumValue](http://venicedb.org/javadoc/com/linkedin/venice/utils/VeniceEnumValue.html), [EnumUtils](http://venicedb.org/javadoc/com/linkedin/venice/utils/EnumUtils.html)
and related unit test classes, which provide a structure to minimize the chance that we mistakenly change the mapping of
numeric ID -> enum value.

### JavaDoc

Speaking of comments, we ideally want JavaDoc at the top of all classes. The top of class JavaDoc should indicate the
set of responsibilities of the class. If the list of responsibilities grows long and/or lacks a common theme, it may be
an indicator that the class ought to be split up.

JavaDoc for functions is desired for public client APIs intended to be leveraged directly by end users. For internal
functions, JavaDoc is desired only if there is something important to call out. Sometimes, a lengthy function JavaDoc
may be an indication that the function's name is not sufficiently clear, or that the complex function should be split 
into multiple simpler (and well-named!) functions.

Use a single-line JavaDoc if it fits, e.g., `/** A very short description */`

Note that we also use JavaDoc everywhere we feel like, not just at the top of functions and classes. This is atypical,
but intentional. It allows us to use the `{@link ClassName}` syntax in order to make the code more easily navigable and
refactorable. If your IDE complains about dangling JavaDocs, that hint should be disabled. If using IntelliJ, this will
be configured automatically when calling: `./gradlew cleanIdea idea`

### Logging

We use log4j2 and want to use interpolation, rather than manual string concatenation, everywhere for efficiency reasons. 
Note that Spotless may break up fragments of strings by concatenating over multiple lines, but that doesn't matter as it
gets optimized away by the compiler. Only concatenations with variables end up carrying an overhead.

Hot path logging should not be above debug level. Keep in mind that exception paths could occasionally turn into hot 
paths. In those cases, we may want to use the `RedundantExceptionFilter`. More generally, hot path logging typically
benefits from being converted into a metric instead.

Do your best to make log messages meaningful. Avoid scary yet imprecise wording. If the situation is dire, let the log
spell out precisely what happened, along with enough context to make debugging easier.

If there is a known solution to remediate the issue, consider why isn't this solution reactively activated so the system 
fixes itself, rather than just logging? If the solution exists but cannot be wired in reactively, then it may be 
desirable for the log to indicate what that solution is, to give the user or operator a clue about what to do next 
(i.e. the _Ghost in the Shell_ design pattern).

### Encapsulation

As much as possible, try to maintain tight encapsulation in all classes. Internal state should be exposed as little as
feasible, and possibly not at all. Consider providing only getters, and not setters, if there is no need for the latter.
Always be careful when returning objects (as opposed to primitives) as these may contain state that can then be mutated
from outside the class they originated from. For example, instead of returning a map, it may be preferable to expose
only a getter for retrieving entries from this map. Alternatively, the map could be placed in a read-only wrapper (but
do consider this option carefully if it is going to happen on the hot path, in which case perhaps the read-only wrapper
could be pre-allocated, especially if the wrapped map is final).

If the API of a class is such that another class needs to call multiple functions in a row to achieve a desired outcome,
then ask yourself whether the calling class is hand holding the internal state of the called class. Would the internal
state of the called class be left in an inconsistent or incoherent state if the calling class stopped halfway through
its sequence of function calls, or if it called those same functions in a different order? If the answer is yes, then
perhaps the multiple functions should be presented as a single public function, which then internally delegates to many
private functions, in the correct order.

Avoid passing `this` into classes as this may make the flow of the code difficult to understand. Also, more generally,
consider whether a class actually needs a handle of an instance of an entire other class (and thus have the ability to 
call any of its functions), or whether it could make do with an instance of a more constrained interface, which the 
other class implements, or perhaps even just a handle to a specific function of the other class (thus limiting the 
surface area of their interaction).

### Avoid Wildcard Imports

We avoid wildcard imports since they may lead to bugs due to pulling in unintended classes or functions. If using 
IntelliJ, the auto-conversion of imports into wildcards can be disabled by following these 
[instructions](https://www.jetbrains.com/help/idea/creating-and-optimizing-imports.html#disable-wildcard-imports). This
would be a good candidate for automation, perhaps via a new Spotbugs plugin; contributions welcome!

### Avoid Optionals

We are aligned with the philosophy of the original creators of the Optional API, which is that it is a useful construct
in the context of the Java 8 stream APIs, but should generally not be used beyond that. Null is a perfectly appropriate
way to denote emptiness, and is not more or less likely to cause a `NullPointerException`. Sentinel values in primitive 
types (e.g., `-1` for a numeric value that is otherwise expected to be positive) are also perfectly appropriate ways to
denote emptiness. For more info, here are a good [video](https://www.youtube.com/watch?v=fBYhtvY19xA&t=2317s) and 
[post](https://homes.cs.washington.edu/~mernst/advice/nothing-is-better-than-optional.html) on this subject.

### Look for Ways to Mitigate Failures

In a system with lots of moving parts, it should be expected that things fail all the time. We should look for design
patterns that help us mitigate failures, wherever possible.

An example of this is the way that dataset versions work in Venice. A dataset version has a unique name, based on the 
dataset name concatenated with a monotonically increasing version number. A given dataset version name is immutable, in
the sense that it will forever point to one and only dataset version, and cannot be reused. Even if a dataset is deleted
and then re-created under the same name, we don't restart the version number sequence, so there cannot be a clash in
the names of dataset versions coming from before and after the dataset re-creation. A dataset version is associated with
various resources including a Kafka topic, a Helix resource, persistent state, and in-memory state. When purging an old
dataset version, if any of the resources that constitute it fail to get cleaned up properly, it doesn't prevent future
dataset versions from continuing to get created, since they should never clash. In this case, therefore, a failure to
delete a resource results not in immediate and widespread systemic failure, but merely in a resource leak, which can be 
monitored, alerted on, and remediated if it creeps beyond a certain threshold.

### Be a Benevolent Tyrant

The CPU will do whatever we tell it, day in day out, without complaints, but it does not mean we ought to abuse it.
Although there is undoubtedly a kernel of truth in the saying that "premature optimization is the root of all evil",
it is important to consider that the reverse is not equally true. In other words, non-optimized code is not the root of 
all clean code. This picture from one of the talks by Java performance expert Aleksey Shipilëv describes the idea in an 
easy to grasp manner:

![Complexity vs Performance](https://user-images.githubusercontent.com/1248632/195111861-518f81c4-f226-4942-b88a-a34337da79e3.png)

For example, if a class contains some final string property, and the code in this class repeatedly performs a lookup
by that property, then it implies that the result of this lookup may change over time. If that is true, then the code is
fine, but if it is not true that the result of the lookup would change over time, then it is simply useless code. Doing
the lookup just once, and caching the result in another final property, makes the code not only faster and more 
efficient, but also easier to reason about, since it indicates the immutability of this looked up property.

Another example is interrogating a map to see if it contains a key, and if true, then getting that key out of the map.
This requires 2 lookups, whereas in fact only 1 lookup would suffice, as we can get a value from the map and then check
whether it's null. Moreover, doing it in 1 lookup is actually cleaner, since it eliminates the race condition where the
lookup may exist during the `containsKey` check but then be removed prior to the subsequent `get` call. Again, the 
faster code is cleaner.

Yet another example is using the optimal data structure for a given use case. A frequent use case within Venice is to
look something up by partition number (which are in a tight range, between zero and some low number), and thus it is
possible do the job with either an int-keyed map or an array. If both work equally well from a functional standpoint, 
then let us use an array, as it is more efficient to perform an index lookup within an array than a map lookup. For 
collections of primitives, it is advised to consider using [fastutil](https://fastutil.di.unimi.it/). If using a more 
efficient data structure requires significant acrobatics, then we may still prefer to opt for the less efficient one, 
for the sake of maintainability (e.g., if it falls within the red zone of the above diagram). That being said, we should 
consider whether we can build a new data structure which achieves both convenience and efficiency for a given use case
(e.g., yellow zone). This kind of low-level work is not considered off-limits within Venice, and we welcome it if there 
is a good rationale for it.

More generally, always keep in mind that the hot path in Venice may be invoked hundreds of thousands of times per second 
per server, and it is therefore important to minimize overhead in these paths. By being benevolent tyrants, our CPUs 
serve us better, and will hopefully care for us when AGI takes over the world.