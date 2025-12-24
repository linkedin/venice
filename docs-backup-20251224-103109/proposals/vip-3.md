---
layout: default
title: VIP-3 Rust Server Read Path
parent: Proposals
permalink: /docs/proposals/vip-3
---

# VIP-3: Rust Server Read Path

* **Status**: _Under Discussion_
* **Author(s)**: _Felix GV_
* **Pull Request**: [PR 604](https://github.com/linkedin/venice/pull/604)
* **Release**: _N/A_

## Introduction

As Venice users need ever-increasing performance, in terms of throughput, average latency and tail latency, it is
necessary to revisit past design choices and implementation details to consider what would it take to reach the next
level. This proposal is submitted in that spirit.

The Fast Client is the most significant change to the read path since the inception of Venice, and it is progressing
well. It helps latency by eliminating the router hop and most importantly by eliminating an entire JVM from the hot 
path. After this, the only JVM left in the backend is that of the server, which this proposal aims to eliminate as well.

The goal is to replace this Java code with another lower-level language which would be free of garbage collection, thus 
making tail latency much more predictable. In addition, this would likely open up the door to increased throughput 
capacity as well.

The specific language proposed to replace Java is [Rust](https://www.rust-lang.org), which has become mature and well 
accepted in the industry over the past years. It promises performance similar to C++, without some of its pitfalls, 
thanks to its borrow checker.

## Problem Statement 

The Venice read path should be completely free of garbage collection. Venice should be able to serve single get read
requests, end-to-end (i.e. as measured by the client), at a p99.99 latency under 1 millisecond.

## Scope

This project can be broken up in several milestones, but we will focus on the most minimal scope which can be shipped
independently of the rest and serve as the MVP version of this initiative.

At a high level, the MVP is to support batch gets, compatible with the Fast Client.

In more details, achieving the MVP requires:

- Bringing Rust/Cargo inside the Venice build.
- Selecting all required crates (probably RocksDB, gRPC and JNI at a minimum).
- Building and publishing a jar which includes the DLL of the Rust server, compiled for Mac and Linux.
- Writing the Rust code which can listen for gRPC requests, query RocksDB and return the response.

Out of scope of the MVP is:

- Read compute support.
- Thin client support.
- Auxiliary endpoints (e.g. metadata, schema, dictionary, etc).
- Everything else that is not part of the read path.

## Project Justification

The demand for AI use cases powered by Venice is growing on a steep trajectory along all dimensions, including the
number of use cases, the queries per second, the keys per query, the number of records and the size of records. While
Venice is holding up for now and we can keep growing clusters and building new ones in the short-term, it is important 
to also take a longer-term view and to kick off initiatives that will improve the foundation of the platform, even if 
those initiatives will only land in production in the next 6-12 months. We need Venice to not only continue to further 
scale horizontally, but to better scale vertically as well. As the footprint of the Venice backend continues to grow, 
the efficiency of each node in the system becomes more important to optimize.

## Functional specification

This is operator-facing work, so by design it should be as transparent as possible to the user. For the operator, the 
new APIs and configs include:

- New server config:
  - To enable the Rust server
  - To specify the port which the Rust server will listen on
  - Other configs of the Rust server implementation (e.g. worker thread count, etc; TBD)
- Other config or store setting (TBD):
  - To determine if cluster discovery will return the Rust server port or the regular port for a given store/cluster
    (or possibly a mix, where only some servers enable the Rust implementation).

From the user's perspective, as long as they are using the Fast Client with the gRPC mode enabled, they should be
eligible to benefiting from the Rust server, if it happens to be enabled by the operator.

## Proposed Design

In a nutshell, the idea is to create a Rust program which wraps RocksDB, and presents to our server JVM the same JNI
APIs which RocksJava has today (or at least the subset we use). In this way, the server continues to use RocksDB as
normal, for both the ingestion path and the legacy read path. However, this Rust program would also open up a port and
listen for gRPC requests, which it would handle and respond to. The diagram below shows the current and proposed
architectures side-by-side.

![Architecture Diagram](../assets/images/vip_3_read_path.drawio.svg)

### Why Rust?

In terms of a GC-free language, the main contender would be C++. We can certainly consider that option as well, but it
seems that nowadays Rust is broadly accepted as a solid alternative, and the momentum behind it is growing. Furthermore, 
Rust's performance already rivals that of C++ in many workloads, and where there are differences one way or the other 
they seem to be quite minor. The benefits of Rust's borrow checker are expected to reduce risk in terms of executing 
this project and operating the end result safely.

In order to write idiomatic, higher quality, Rust code, the project will adopt [rustfmt](https://github.com/rust-lang/rustfmt) 
and [rust-clippy](https://github.com/rust-lang/rust-clippy).

### Why gRPC?

Because it seems easier than to re-implement Venice's bespoke [REST spec](../dev_guide/router_rest_spec.md). Also,
although gRPC is probably not perfect, it is likely more efficient than the current REST protocol, and will make it
easier to support clients in other languages. Also, we already have gRPC embedded in the stack following the Summer 2023
internship project, so it might as well be leveraged.

There is more than one crate option for integrating with gRPC, but the most mature one seems to be [Tonic](https://crates.io/crates/tonic).

### Why JNI?

Because it is supported in Rust, and that should make the integration easier. An alternative would be to have the Java 
server communicate with the Rust server via some other mechanism (loopback socket, pipes, etc) but then that would mean
adding lots of forks in the code, and might also be less efficient. Using a non-JNI mechanism would also mean that just
enabling the Rust server (even if it's not used yet) would already alter the behavior of the main code, thus creating
more risk. In order to work in an incremental manner, where the Rust code can be developed in small chunks and hidden
behind a config flag until ready, it is preferable to keep this interface the same.

In the end state, JNI would only be used by the Java code responsible for the ingestion, and the read path would be
fully JNI-free. Also, it should be noted that the Rust server will not be a separate child process spawned by the Java
process, but rather fully embedded in the Java process. The Rust server will be activated by a JNI call initiated by the
Java server, which spins up the gRPC Handler on the Rust side, along with any port and threads it needs to function.

Doing JNI properly requires packaging a jar with code compiled for multiple architectures (at least Mac and Linux for
the time being). There is some complexity there, but it can be done.

Although there are many tutorials on how use JNI with Rust, we can consider using a higher-level framework to facilitate
the work, such as [duchess](https://github.com/duchess-rs/duchess).

### Quota

The read quota will need to be re-implemented in Rust. There are essentially three approaches we can consider:

1. Let the Rust server and Java server share a quota amount between them, so that no matter if the traffic comes in via
   the Java port or the Rust port, it will be accounted for and the configured limit will be respected. This requires
   having some kind of synchronization between the two sides (either at the granularity of each request, which is likely
   too expensive, or by reserving some allotment periodically, which is complicated).
2. Let the Rust server and Java server each have the full quota amount and not need any interaction between each other
   for sharing it. This is much simpler to implement, but the downside is that if clients were to perfectly balance
   their traffic across the two ports, they could get up to twice their configured quota.
3. If the Rust server is enabled, then completely disable the ability of the Java side to answer requests. This would
   uphold the quota limit strongly, without the complexity of quota sharing described in approach 1. The downside is
   that if the Rust server supports only a subset of operations (e.g. lack of Read Compute support), then unsupported
   operations would become completely unavailable on the cluster(s) where the Rust read path is enabled.

Since the migration is expected to be controlled by the operator, and should only be activated for users of the Fast
Client (i.e. not for users that run a mix of Thin Clients and Fast Clients), the edge case solved by the more
complicated solution 1 does not seem likely to come up. Therefore, the proposal is to pick either solution 2 or 3.

Also, it can be considered whether quota should be part of the MVP at all. If the Rust server mode will be tried in 
dedicated clusters at first, then quota may not even be a must-have (though of course it will be needed eventually).

### Cluster Discovery

The general idea is that when the Rust mode is activated in a server, that server would announce that it has a gRPC 
endpoint at the port which the Rust server listens to. The announcement can be done by the Java server, to minimize
complexity on the Rust side. Beyond that, there are many variants on how to make things work end-to-end.

A few alternatives:

1. The Java and Rust servers are all announced to the same resource name. This means there is some gRPC resource which
   a Fast Client can hit, and that could be served either by Java or Rust, and possibly both. Changing which path the
   clients go through would require turning on or off the announcement on the Java and/or Rust side, which could be
   done either via reconfiguring and bouncing the server, or via some admin tooling.
2. The Java and Rust servers are announced to different resource names. When cluster discovery happens, the client asks
   where to connect, the backend could decide to return one or the other of these resources, thus directing the traffic
   fully (at least for that one client instance) into one path or the other. Changing which path the clients go through
   would require updating the mapping returned by cluster discovery, which would incur the same delay as cluster 
   migration.
3. Instead of announcing per-cluster resources, we could announce per-store resources, and then have some admin tooling
   to enable and disable the announcement for Rust and Java servers on a per-store basis. This could give the most
   flexibility, since the client would not remain stuck with whatever resource name was returned by the cluster 
   discovery call done at startup time (and periodically thereafter), but rather could adapt rapidly.

These options provide different levels of granularity of control and speed of change, with finer granularity and faster
change carrying greater complexity cost.

### Auth/Auth

The Rust server will open a port and receive connections from clients, so it will need to authenticate and authorize
them. Currently, the auth/auth logic is all in Java, so there needs to be a solution to doing it on the Rust side. There
are two main alternatives:

1. Design an abstraction which can be implemented by a custom Rust module, so that the Rust server can interrogate an
   API and find out if a given principal/token/etc is allowed to access a given resource.
2. Implement a mechanism by which the Rust server can call into the Java server, ask whether a given principal/token/etc
   is allowed to access a given resource, let the Java server answer that with the current abstraction that exists over
   there, return that response to Rust, and finally let Rust cache this decision so that known principals need not do
   this again. In this scheme, there needs to also be a mechanism to periodically refresh the authorization decision
   (e.g. once a minute), in case it may have changed.

Given that Venice already requires the operator to provide a Java implementation of the authorization implementation, it
is probably easier to leverage this one and only implementation in Rust as well (via approach 2) rather than require the
operator to implement their authorization in two places. Furthermore, approach 2 has the potential to be more efficient,
since the authorization logic need not run within the hot path of every single request. For these reasons, approach 2 is
preferred.

### Metrics

Similarly to auth/auth, there is already an abstraction for taking metrics out of Venice on the Java side, so for the
metrics that will be gathered on the Rust side, two analogous approaches exist: build a Rust abstraction that can be
implemented so that the Rust server gains the ability to emit metrics directly, or build a mechanism by which metrics
held in Rust's memory can be copied over to the Java side, and emitted alongside the other Java side metrics. The latter
approach is analogous to what is being done in Ingestion Isolation, where the child process periodically sends all its 
metrics to the parent process. For the same reason as in auth/auth that it would be simpler for the operator, the second 
approach is preferred here as well.

## Development Milestones

The milestones for the MVP would be the following:

1. Build a simple RocksDB wrapper which gets packaged into a jar and exposes the same JNI APIs as RocksJava. This can be
   built with a single architecture at first. Print "hello world" when JNI is called successfully.
2. Swap RocksJava for the wrapper inside Venice. We can do a micro-benchmark at this stage to validate the assumption 
   that JNI via Rust is no worse than JNI via C++.
3. Integrate the chosen crate for adding gRPC support, and do a "hello world" of the RPC.
4. Add server configs to enable the Rust gRPC module (including basic service disco announcement).
5. Implement the batch get support.
6. Implement support for auth/auth.
7. Implement support for metrics.
8. Multi-architecture (Linux + Mac) support.
9. (Possibly optional) implement quota.
10. (Possibly optional) more advanced/granular service disco control.

Beyond the MVP, the following scope expansions are natural followups:

1. Quota (if excluded from the MVP).
2. Bidirectional streaming for batch get (if not implemented in the MVP).
3. Read compute support.
4. Support auxiliary endpoints (metadata, schema, etc).

Other ideas for leveraging Rust, which are less likely to happen, but could eventually be considered:

1. Rust Fast Client which Java (and possibly other languages) could call via JNI (or similar), so that as much of the
   end-to-end transmission of data can be GC-free, with only the very last mile being done in the user app's language.
2. Rust Router. This is an alternative to the idea 1 above, in case we would want to keep the Thin Client as a part of 
   the architecture in the long-term, perhaps as a way to implement support for clients in other languages more easily.
3. Rust ingestion. This would be quite complicated, since the Venice ingestion is very complex and sophisticated. It
   could possibly be broken up into increments, e.g. we might start off with Write Compute being done in Rust, and 
   exposed as a single JNI call to Java (as opposed to the multiple calls it currently needs).
4. Rust controller. This is no doubt the least attractive milestone, since the controller is fairly complex, and not
   performance-sensitive. Almost certainly wouldn't happen.

## Test Plan

We of course need Rust unit tests for the Rust code.

All Fast Client integration tests written in Java should get an extra permutation for enabling the Rust server.

Besides that, there will need to be extensive pre-prod certification to minimize the risk that enabling the Rust server 
could destabilize the Venice server.

## References 

Material for learning Rust:

- [The Book](https://rust-book.cs.brown.edu)
- [Rust Atomics and Locks](https://marabos.nl/atomics/)
- [The Rustonomicon](https://doc.rust-lang.org/nomicon/) (probably unnecessary for this project, but just in case...)
