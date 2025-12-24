# Java
{: .no_toc }

The Venice codebase is completely in Java, and therefore it is important to have at least a basic understanding of the 
language and its runtime, the Java Virtual Machine (JVM). This page is not intended to be an in-depth resource on Java, 
but might be a useful starting point for Java beginners. Links to more in-depth resources will be provided along the 
way, and in cases where there are many distinct ways of using Java, guidance will be provided in terms of the Venice 
project's expectations.

## Table of Contents
{: .no_toc }

- TOC
{:toc}

## Java in a Nutshell

Java as a language is statically typed, dynamically compiled, object-oriented, and provides managed memory. Let's 
briefly go over each of these buzzwords.

### Statically Typed

Static typing, also known as strong typing, is the notion that a variable is declared to have a given type, and that the
language provides guarantees that it is only possible to assign data to this variable which conforms to the declared 
type. This guarantee is ensured at compilation-time, which is good in the sense that compilation happens early in the
development lifecycle, therefore preventing trivial mistakes from creeping into production systems and being discovered
there, at run-time, when it is already too late. Static typing therefore provides a maintenance benefit

In addition, static typing also provides a performance benefit, because the compiled code can make certain assumptions
which a dynamically typed language could not. This enables certain low-level optimizations to take place.

### Dynamically Compiled

Although Java programs need to be compiled, they are not compiled directly to machine code, but rather to an 
"intermediate representation" called bytecode. The bytecode can then be loaded into a Java Virtual Machine (JVM), which
interprets it, and dynamically compiles architecture-specific code to run it. This means that Java code is portable
("compile once, run everywhere"), though in practice, in the case of Venice, the code has been exercised mainly on Linux
(for production usage) and macOS (as a development environment). In theory, it should work on Windows or other platforms
though it has not been tested there.

An additional benefit of dynamic compilation is, again, performance. The JVM features a JIT (Just-in-time) Compiler 
which uses heuristics to analyze which code paths are most frequently called. These code paths end up being recompiled
into even more optimal versions.

### Object-Oriented

Java at its core is an object-oriented language, although over the years it has also added certain other paradigms such
as functional programming. What it means concretely is that design patterns such as encapsulation, inheritance and 
polymorphism are well-supported.

### Managed Memory

An important aspect of Java is that memory management is abstracted away from the programmer. In particular, there is no
need to explicitly _free_ allocated memory, and there is no risk of accidentally accessing a memory address after it has
been freed. This design improves both reliability and security, however, it comes at the cost of needing garbage 
collection, which in some cases is a performance drag. These various aspects are explored more in-depth in later 
sections.

## Other Languages

Before diving deeper into Java itself, it may be good to point out the role of some other languages. Although the Venice
codebase is in Java, some of Venice's dependencies are not in Java.

Furthermore, there may be other languages introduced to the Venice codebase in the future, for example Python tooling or
[Rust server code](../proposals/vip-3.md).

### C++

There are two major dependencies which are written in C++, and which are accessed via the [Java Native Interface](https://docs.oracle.com/en/java/javase/17/docs/specs/jni/intro.html)
(JNI):

- The [RocksDB](https://rocksdb.org) storage engine.
- The [ZSTD](https://facebook.github.io/zstd/) compression algorithm.

### Scala

Some other Venice dependencies are written in [Scala](https://www.scala-lang.org), a language which gets compiled into
bytecode and runs within the JVM, alongside the bytecode compiled from Java.

Although inter-operability between Java and Scala is supported, there are nonetheless certain challenges associated with
the use of Scala due to the incompatibility across minor versions and the need to cross-compile Scala dependencies
across a range of minor versions.

For this reason, the Venice project intends to not make use of any Scala within its codebase, and to limit the use of
Scala dependencies to the most minimal number of modules possible. In particular:

- The [Kafka](https://kafka.apache.org) broker is written in Scala, but not the Kafka client library. The broker is only
  used within integration tests, and the main code should not depend on it.
- The [Spark](https://spark.apache.org) framework is a runtime dependency of the [Venice Push Job](../../user-guide/write-apis/batch-push.md),
  and therefore VPJ is the only production module which carries a (transitive) dependency to Scala.

### Groovy

The build system, [Gradle](https://gradle.org), functions with the [Groovy language](https://groovy-lang.org), and so
the build files of each module are written in that language. Like Scala, Groovy is also a language that runs within the
JVM, and is interoperable with Java. We do not intend to use Groovy outside of build files.

### Javascript

Finally, there is even a tiny bit of Node.js running in some of the GitHub Actions plugins, used to generate reports! It
takes a village to raise a distributed database, and Venice is no exception! Like Groovy, this is a build dependency and
there is no intent to use JS anywhere else in the codebase.

## Java Versions

Currently, the Venice project intends to support Java versions 8, 11 and 17. This means that the codebase cannot use
language features that have been introduced in versions later than 8. We hope to lift this restriction in the future
and drop support for older Java versions. For the time being, the build ensures that compilation will fail if recent
language features are used.

The main motivation for maintaining compatibility with older Java versions is to support legacy applications which 
depend on the Venice client libraries. The Venice services (controller, router and server) also work on older Java 
versions though in practice we run them on Java 17 JVMs, which provide better performance. As much as possible, it is
recommended to run all Venice code on the latest supported JVM version.

## Memory

Memory is a big subject for any programming language, and Java is no exception. It is important to understand the basic
characteristics of how memory is allocated, garbage collected, laid out, measured and accessed.

### Memory Allocation

Memory can be allocated either on the Java _stack_, the Java _heap_ or in _native_ memory.

Allocations to the stack include all local variables of primitive types (boolean, int, float, etc.) as well as pointers
to objects. [Local variables](https://docs.oracle.com/javase/tutorial/java/nutsandbolts/variables.html) are those 
defined within the scope of a function and are therefore inaccessible outside that scope.

Allocations on the heap include all objects, as well as the fields and pointers they contain.

Allocations to native memory are special in the sense that they require extra care (see Garbage Collection, below). 
Within the Venice codebase, there is no explicit allocation of native memory, but some of Venice's dependencies do use
native memory, for example [Netty](https://netty.io) and RocksDB.

### Garbage Collection

Garbage Collection (usually referred to as GC) is an important aspect of Java. The JVM provides "managed memory", 
meaning that memory is automatically allocated and deallocated as necessary, with minimal effort from the developer.
Although this is ergonomically useful, it is still important to understand the basic principles of GC as it can have
performance implications, and Venice is a performance-sensitive project. Let's take the stack, heap and native 
allocations introduced previously and see how each of them behaves in terms of GC.

Memory allocated on the stack is automatically deallocated as soon as it goes out of scope (e.g. when the function, or
if block or while loop in which it was allocated finishes). This makes stack allocations very efficient since there is
not "leftover" work to take care of (see below).

Memory allocated on the heap, on the other hand, can (at least [potentially](https://blogs.oracle.com/javamagazine/post/escape-analysis-in-the-hotspot-jit-compiler))
have a lifespan greater than the scope within which it was allocated. For example, if an allocated object is assigned to 
the field of another object, then the lifecycle of the former object becomes tied to that of the latter one. These 
objects on the heap eventually need to be cleaned up, which is the responsibility of the Garbage Collector and is 
accomplished asynchronously via background threads, reference counting and other mechanisms. The details are outside the 
scope of this wiki page, but suffice to say that GC has a cost, and so we do care about minimizing garbage generation in 
performance-sensitive hot paths.

Finally, native memory allocation requires manual memory management, as the JVM will not take care of automatically
deallocating it as part of GC. This is the reason why it is important, for example, to close objects returned by RocksDB
when they are no longer in use, and to invoke the [ref counting APIs of Netty](https://netty.io/wiki/reference-counted-objects.html) 
when appropriate.

#### Memory Leaks

Memory allocated on the heap or natively can leak, which causes severe operational issues.

In the case of heap allocations, a leak can occur if references to unneeded objects are maintained. This results in GC
not being capable of eliminating this garbage since the ref counting concludes that the referenced objects are 
ineligible for collection. After the heap fills up, further allocations will result in [OutOfMemoryError](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/OutOfMemoryError.html) 
getting thrown. In order to debug this type of issue, it is useful to configure the JVM with the 
`-XX:HeapDumpOnOutOfMemoryError` config flag, and conduct heap dump analysis. See these [mem leak troubleshooting tips](https://docs.oracle.com/en/java/javase/17/troubleshoot/troubleshooting-memory-leaks.html)
to dig deeper.

In the case of native allocations, a leak can occur if the Java code fails to explicitly release the objects. This can
ultimately cause the process to get terminated by the OS' OOM Killer.

### Memory Layout

It can be useful to understand how objects are laid out in memory, in order to get a sense of the overhead of the data
structures we design. In general, the main design criteria is simply to adhere to object-oriented principles in order to
achieve encapsulation and other characteristics of interest. But in cases where some pieces of data need to be allocated
on the hot path (i.e., at high throughput), or be retained for long periods of time, it can be useful to pay attention
to what the heap size of the data is going to be, with an eye towards minimizing its footprint. This section is a just a 
high-level overview, and more details can be found in [Java Objects Inside Out](https://shipilev.net/jvm/objects-inside-out/).

#### Memory-Impacting Configurations

Note that memory layout details are dependent on which version of the JVM is used, as well as various JVM settings. In
order to facilitate explanations throughout this whole section, we will provide concrete memory size numbers based on a 
specific (though, arguably, fairly common) set of JVM configurations:

- 64 bits JVM
- Java 17
- Max heap size of less than 32 GB
- All other memory-related settings on their default values, including:
  - Compressed pointers (enabled)
  - Compressed class pointers (enabled)
  - Alignment size (8 bytes)

#### Object Headers

All objects in Java have a header, which is made up of a mark word and a class pointer. The mark word contains various
JVM internal details such as GC-related bookkeeping, the identity hashcode, and locking information, while the class 
pointer is a reference to the memory address of the class definition inside the metaspace.

Assuming the above JVM configuration details, the object layout will carry 12 bytes worth of header, and therefore look 
like this:

```
|           mark word           | class pointer |     fields    |
|<---------- 8 bytes ---------->|<-- 4 bytes -->|<-- N bytes -->|
```

In the case of arrays, there is one more header field, which is an `int` storing the length, and that is followed by 
each element stored in the array (either primitives stored inline, or pointers to Objects stored elsewhere on the heap).
The array, therefore, has a 16 bytes header, which looks like this:

```
|           mark word           | class pointer |     length    |    elements   |
|<---------- 8 bytes ---------->|<-- 4 bytes -->|<-- 4 bytes -->|<-- N bytes -->|
```

#### Alignment

The previous subsection taught us that (under the example JVM settings above) an Object's header takes up 12 bytes, 
however, even if this Object carried no fields at all, its actual size on the heap would still be larger than 12 bytes! 
That is because objects are "aligned" in memory, and the alignment size (under default JVM settings) is 8 bytes. 
Therefore, even the basic Java `Object` with no fields in it, having just a 12 bytes header, would actually take up 16 
bytes on the heap:

```
|           mark word           | class pointer |     wasted    |
|<---------- 8 bytes ---------->|<-- 4 bytes -->|<-- 4 bytes -->|
```

From this, we can derive a rule of thumb that every time we use an Object, we are spending at least 16 bytes, and if the
object carries a non-trivial number of fields, then likely more than that.

More nuances on alignment and its downstream effects can be learned in [JVM Anatomy Quark #24: Object Alignment](https://shipilev.net/jvm/anatomy-quarks/24-object-alignment/).

#### Size of Primitives

Below is the size in memory of all primitive types:

| Primitive type | Size in bytes |
|----------------|---------------|
| `boolean`      | 1             |
| `byte`         | 1             |
| `char`         | 2             |
| `short`        | 2             |
| `float`        | 4             |
| `int`          | 4             |
| `double`       | 8             | 
| `long`         | 8             |

Note that all of the above are compact representations, meaning that every single bit carries a significant piece of
information, except for `boolean` which carries only one significant bit out of the 8 bits that make up its byte.

Even when part of a `boolean[]` primitive array, each element will still take 1 byte. This JVM design choice makes 
certain operations more efficient for the CPU to perform, and protects against [word tearing](https://shipilev.net/blog/2014/jmm-pragmatics/#_part_ii_word_tearing),
at the cost of memory waste. In cases where the opposite tradeoffs are desired, it is possible to use a [BitSet](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/BitSet.html), 
which is more space-efficient thanks to "bit packing", and provides support for bit-related operations such as `AND`, 
`OR`, `XOR`, intersection, etc.

#### Size Comparison Between Primitives and Objects

For each of the primitive types, there exists an equivalent Object type. Using the Object carries the header overhead,
while the primitive type does not. Continuing with the example JVM settings above, we can see that any primitive equal
or smaller to an `int` can fit "within the shadow" of the object alignment, and therefore takes up 16 bytes in Object 
form. Whereas the `double` and `long` primitives, if packed inside of a `Double` or `Long` object, will need 20 bytes,
and therefore be rounded up to 24 bytes due to alignment. And so we can see that the overhead of using an Object rather
than a primitive is:

| Object type | Size in bytes | Overhead compared to primitive |
|-------------|---------------|--------------------------------|
| `Boolean`   | 16            | 16x                            |
| `Byte`      | 16            | 16x                            |
| `Character` | 16            | 8x                             |
| `Short`     | 16            | 8x                             |
| `Float`     | 16            | 4x                             |
| `Integer`   | 16            | 4x                             |
| `Double`    | 24            | 3x                             |
| `Long`      | 24            | 3x                             |

The above is a "theoretical worst case". If using the recommended factory methods (e.g., the various `valueOf` 
overloads), then there is a possibility of leveraging singleton instances coming from a central cache. These cached
instances cover both `Boolean` values, all `Byte` values, and up to 256 values (those closest to zero) of all other
non-floating point types. If the cache is used, then effectively the heap size of these instances is zero (i.e., 
assuming that they are part of the "base overhead" of the JVM itself, and that they can be amortized across many 
references), and therefore their memory cost is only that of the reference itself, which brings us to the following 
point.

An object allocated on the heap is not just floating there on its own, it has at least one pointer referencing it 
(otherwise it is unreachable and therefore eligible to garbage collection). That pointer might be on the stack, or it 
might be a field of some other object. Continuing with the example JVM settings above, we would be operating in the
"compressed pointers" mode, meaning that they would take up 4 bytes each (if the max heap size is >= 32 GB, then 
pointers cannot be compressed and take up 8 bytes each). And so the size of the pointer needs to be added to the heap 
size of the object itself, in order to calculate its full memory cost.

If we assume the use of recommended factory methods, and we add the pointer size, then we get the following sizes:

| Object type | value     | Size in bytes | Overhead compared to primitive |
|-------------|-----------|---------------|--------------------------------|
| `Boolean`   | all       | 4             | 4x                             |
| `Byte`      | all       | 4             | 4x                             |
| `Character` | 0..128    | 4             | 2x                             |
| `Character` | others    | 20            | 10x                            |
| `Short`     | -128..127 | 4             | 2x                             |
| `Short`     | others    | 20            | 10x                            |
| `Float`     | all       | 20            | 5x                             |
| `Integer`   | -128..127 | 4             | 1x                             |
| `Integer`   | others    | 20            | 5x                             |
| `Double`    | all       | 28            | 3.5x                           |
| `Long`      | -128..127 | 4             | 0.5x (interestingly!)          |
| `Long`      | others    | 28            | 3.5x                           |

### Why Ever Use Objects Rather than Primitives?

Given the significant overheads described above, it is fair to ask why would we ever want to use the Object version of
these types, rather than the primitive version. There are a few reasons:

1. Objects can be null, while primitives cannot. In some cases, the full range of values provided by a primitive type 
   are needed for a given use case, and in addition to that, we also need to model the absence of a value. For example,
   some use case may require a given field to be not only true, or false, but also absent. This could be modeled by two
   primitive boolean fields, e.g., having both `boolean configValue` and `boolean configValueIsSet`, though that could
   be hard to maintain. Alternatively, we could have a single `Boolean configValue` field, which has the three possible 
   states we care for (true, false, null). Note that `Optional` is one more way of modeling absence, though in practice
   it is not terribly useful, and [we choose to avoid it in the Venice project](../development/style-guide.md#avoid-optionals).

2. Objects can be used as a type parameter in a class which has [Generic Types](https://docs.oracle.com/javase/tutorial/java/generics/types.html),
   while primitives cannot. For example, a `List<Integer>` is valid, while a `List<int>` is not. That being said, in the 
   case of common collections, there is an alternative, provided by one of the dependencies of the Venice project called 
   [fastutil](https://fastutil.di.unimi.it). In the previous example, `fastutil` could be used to provide an [IntList](https://javadoc.io/doc/it.unimi.dsi/fastutil/latest/it/unimi/dsi/fastutil/ints/IntList.html), 
   which is an implementation of `List<Integer>` with additional methods that prevent "boxing" (see below).

#### Boxing and Unboxing

"Boxing" is the term used to designate Java's support for implicitly converting a primitive type to its Object 
equivalent. "Unboxing" is the reverse process, where an Object is transformed into a primitive (though one must be 
careful to guard against the fact that unboxing a null Object results a `NullPointerException`). For example, the 
following code is valid:

```java
class Sample {
  Integer boxingExample(int primitiveInt) {
    Integer objectInteger = primitiveInt;
    return objectInteger;
  }

  int unboxingExample(Integer objectInteger) {
    int primitiveInt = objectInteger; // Warning! This can throw a NullPointerException!
    return primitiveInt; 
  }
}
```

In some cases, the JIT Compiler can optimize away certain object allocations via _scalar replacement_, and boxing can 
sometimes be eliminated in this fashion. It should not be assumed, however, that such optimization can take place (nor 
that it will even if it can). More details in [JVM Anatomy Quark #18: Scalar Replacement](https://shipilev.net/jvm/anatomy-quarks/18-scalar-replacement/).

In general, therefore, it is recommended to avoid implicit boxing in hot paths where it can be reasonably avoided (i.e., 
without undue maintenance burden).

### Estimating the Heap Size of Objects

The above offers some "rule of thumb" guidance for estimating the heap size of certain kinds of objects, in the hope 
that it may help developers make informed decisions about class design. But there are also some scenarios within the
Venice codebase where we wish to estimate the size on heap of objects at runtime. For example, in the ingestion code,
there is some buffering where we wish to deterministically enforce a cap on the size in memory of buffered objects.

There are a few different ways in which we might assess object size in Java, including empirical and theoretical 
approaches.

#### Empirical Measurement

Empirical approaches aim to measure the "true size" of objects. Broadly speaking, we could divide this solution space
into two categories, presented below (though each of those could have many variants).

One such way, which we might qualify as a "black box approach", is to ask the JVM how much memory it is using, then 
allocating the object of interest, then asking the JVM again how much it's using, and calculating the delta between 
before and after. In order to reduce background noise, the allocation step would need to produce a large number of 
instances, and hang on to them, so they don't get garbage collected right away. This type of empirical approach is not 
very practical within production code (due to inefficiency and occasional imprecision), but we do use it in unit tests 
to validate how well the other methods are working.

Another way is that some JVM flags or JVM agents can be configured which provide access to measurement functions. This 
bucket of experimental approaches are expected to provide full precision. They are also more efficient than the black 
box approach (given that there is no need to synthetically allocate a bunch of objects), though it is still not 
completely efficient due to needing to rely on [reflection](https://docs.oracle.com/javase/tutorial/reflect/). Moreover,
the requirement to use certain JVM configurations makes these approaches not fully reliable. In Venice's case, given 
that measurement was needed in some client library (the Da Vinci Client), where the JVM settings can be quite varied 
across the user base, this was not considered sufficiently reliable.

#### Theoretical Prediction

Rather than empirically measuring, we can use theoretical knowledge of JVM internals to "predict" what the size of a 
given object _should be_. The downside of this approach is that, given that it is not empirical, it is possible the
theoretical assumptions it is based on are wrong, therefore leading to imprecision. We can further break down this 
bucket into two approaches:

1. **Generic**, where any object of any class can be measured. This kind of approach may be most convenient to 
   developers but has the downside of requiring reflection, which is not the most efficient. Moreover, achieving 
   precision requires taking into account whether referenced instances are "shared" (in which case they should not be 
   counted) or whether, on the contrary, they "belong" to the referencing object (in which case they should be counted). 
   Keeping track of this phenomenon is costly, both in terms of space and time. For these reasons, the generic 
   approach was avoided for Venice's measurement use cases.

2. **Specific**, where only certain classes can be measured. This is the approach chosen in Venice, and it takes the 
   form of classes implementing the `Measurable` interface. These classes implement a `getHeapSize` function which can 
   leverage knowledge of the Venice implementation details to skip counting any shared instances, and count 
   everything else efficiently (i.e., without reflection on the hot path). The downside of course is that this 
   approach requires more maintenance cost. The utilities in the [com.linkedin.venice.memory](https://venicedb.org/javadoc/com/linkedin/venice/memory/package-summary.html) 
   package make it easier to implement `Measurable` correctly, though some manual work is still required.

### Concurrent Access to Memory

Java provides first-class support for concurrent workloads. The standard library includes many facilities in the
[java.util.concurrent](https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/util/concurrent/package-summary.html)
package for writing multithreaded applications. When writing such code, it is often necessary for different threads to
access shared state. In order to do this correctly, it is important to understand the [Java Memory Model](https://docs.oracle.com/javase/specs/jls/se17/html/jls-17.html).

Although a full exploration of this is beyond the scope of this page, a few basic principles can be summarized:

1. Code is not guaranteed to be executed in the order it is written. Due to various optimizations, certain operations 
   which are written sequentially in the code can be re-ordered, or executed in parallel. Occasionally, this can produce
   surprising results.

2. Memory is not a single entity, but rather many layers of cache, and caches can be incoherent. There is RAM, which 
   contains the "authoritative" value of a given piece of state, and then there are multiple levels of CPU cache which 
   copy some subset of the RAM, in order to make it more efficient for the CPU to access it. Threads running on 
   different CPUs may have access to different copies of the RAM, and therefore see inconsistent results.

3. In order to achieve correctness when handling state which is read and written by multiple threads, we must give
   explicit instructions to Java. For example:

   1. The `volatile` keyword prepended before a field indicates that reads and writes to that field are guaranteed to be
      consistent. Although the way this is guaranteed is not officially specified, in practice it has been 
      [experimentally determined](https://github.com/alex-dubrouski/java-test?tab=readme-ov-file#volatiles-test) that 
      volatile writes cause all CPU caches to get invalidated, while the volatile reads can still benefit from CPU 
      caches. Many of the `java.util.concurrent` classes rely on `volatile` fields to implement various functionalities 
      such as _Compare and Swap_, locking, etc.
   
   2. The `final` keyword prepended before a field indicates that it is immutable, and therefore safe to cache in the 
      CPU. Note, however, that there is a subtlety. Only a primitive final field is "fully immutable", whereas an Object 
      final field merely means that the pointer is immutable (i.e., always referencing the same instance on the heap) 
      while the fields within the referenced instance can themselves be mutable.
   
   3. The `synchronized` keyword can be used to indicate that only one thread at a time is allowed to enter the 
      designated critical section. This can make it so that a non-`volatile` field is accessed in a threadsafe way, but
      only if all such accesses are guarded by synchronization.

## Conclusion

This page is merely a small selection of Java-related topics. Ultimately, learning the language well requires reading 
and writing a lot of code.

Although most of the above content is general in nature, and only occasionally makes references to Venice-specific 
details, a Venice developer should also get familiar with the rest of the project-specific recommendations found in the 
[Style Guide](../development/style-guide.md).