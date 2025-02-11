---
layout: default
title: Navigating the Project
parent: Developer Guides
permalink: /docs/dev_guide/navigating_project
---
# Navigating the Project

The Venice codebase is split across these directories:

- `clients`, which contains the user-facing libraries that most Venice users might be interested in. Those include:
   - `da-vinci-client`, which is the stateful client, providing "eager caching" for the Venice datasets. [Learn more](../user_guide/read_api/da_vinci_client.md).
   - `venice-admin-tool`, which is the shell tool intended for Venice operators.
   - `venice-client`, which is a one-stop-shop for all clients which an online application might need, including 
      thin-client, fast-client, da-vinci-client, consumer.
   - `venice-producer`, which enables an application to perform real-time writes to Venice.
   - `venice-push-job`, which enables an offline job to push batch data to Venice.
   - `venice-thin-client`, which is the most minimal dependency one can get to issue remote reads to the Venice backend,
     by delegating as much of the query logic as possible to the Venice router tier. 
- `integrations`, which contains additional libraries that some Venice users might be interested in, to connect Venice
  with other third-party systems. The rule of thumb for including a module in this directory is that it should have
  minimal Venice-specific logic, and be mostly just glue code to satisfy the contracts expected by the third-party 
  system. Also, these modules are intended to minimize the dependency burden of the other client libraries. Those 
  include:
  - `venice-beam`, which implements the Beam Read API, enabling a Beam job to consume the Venice changelog.  
  - `venice-pulsar`, which contains an implementation of a Pulsar [Sink](https://pulsar.apache.org/docs/next/io-overview/#sink),
    in order to feed data from Pulsar topics to Venice.
  - `venice-samza`, which contains an implementation of a Samza [SystemProducer](https://samza.apache.org/learn/documentation/latest/api/javadocs/org/apache/samza/system/SystemProducer.html),
    in order to let Samza stream processing jobs emit writes to Venice.
- `internal`, which contains libraries not intended for public consumption. Those include:
   - `alpini`, which is a Netty-based framework used by the router service. It was forked from some code used by 
     LinkedIn's proprietary [Espresso](https://engineering.linkedin.com/espresso/introducing-espresso-linkedins-hot-new-distributed-document-store) 
     document store. At this time, Venice is the only user of this library, so there should be no concern of breaking
     compatibility with other dependents.
   - `venice-client-common`, which is a minimal set of APIs and utilities which the thin-client and other modules need 
     to depend on. This module used to be named `venice-schema-common`, as one can see if digging into the git history.
   - `venice-common`, which is a larger set of APIs and utilities used by most other modules, except the thin-client.
- `services`, which contains the deployable components of Venice. Those include:
  - `venice-controller`, which acts as the control plane for Venice. Dataset creation, deletion and configuration, 
    schema evolution, dataset to cluster assignment, and other such tasks, are all handled by the controller.
  - `venice-router`, which is the stateless tier responsible for routing thin-client queries to the correct server 
    instance. It can also field certain read-only metadata queries such as cluster discovery and schema retrieval to
    take pressure away from the controller.
  - `venice-server`, which is the stateful tier responsible for hosting data, serving requests from both routers and
    fast-client library users, executing write operations and performing cross-region replication.
- `tests`, which contains some modules exclusively used for testing. Note that unit tests do not belong here, and those 
  are instead located into each of the other modules above.

Besides code, the repository also contains:

- `all-modules`, which is merely an implementation detail of the Venice build and release pipeline. No code is expected
  to go here.
- `docker`, which contains our various docker files.
- `docs`, which contains the wiki you are currently reading.
- `gradle`, which contains various hooks and plugins used by our build system.
- `scripts`, which contains a few simple operational scripts that have not yet been folded into the venice-admin-tool.
- `specs`, which contains formal specifications, in TLA+ and FizzBee, for some aspects of the Venice architecture.

If you have any questions about where some contribution belongs, do not hesitate to reach out on the [community slack](http://slack.venicedb.org)!