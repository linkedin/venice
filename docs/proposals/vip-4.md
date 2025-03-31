---
layout: default
title: VIP-4 Store Lifecycle Hooks
parent: Proposals
permalink: /docs/proposals/vip-4
---

# VIP-4: Store Lifecycle Hooks

* **Status**: _Accepted_
* **Author(s)**: _Felix GV_
* **Pull Request**: [PR 881](https://github.com/linkedin/venice/pull/881)
* **Release**: _N/A_

## Introduction

The [Venice Push Job](../user_guide/write_api/push_job.md) takes data from a grid and pushes it to a store in all 
regions. This works fine in many cases, but there are some use cases where we would like to have greater control over 
the steps of the process. This proposal is to add new configs and hooks which can be used both to monitor and control 
the push job in a finer-grained manner than is possible today. In particular, this proposal focuses on the way that each 
individual region is handled.

Currently, the sequence of steps happening within a push job is as follows:

![Push Job Steps](../assets/images/push_job_steps.drawio.svg)

A few notes on the above diagram:

* A new schema will be registered only if the data being pushed does not conform to any already known schema, and 
  the schema auto-registration config is enabled. If that config is disabled, then an unknown schema leads to job 
  failure.

* The compression job is optional, and whether it runs or not depends on configurations.

* The data push job writes to a store-version pub sub topic in one of the regions (typically the one which is local to
  where the push job is running), but all regions start ingesting right away, as soon as the data push job begins 
  writing. For regions that are remote from the topic the data push job is writing to, the leader servers are performing 
  replication, while in the region which contains that topic, the replication is a no-op.

* The `SERVING` step is also called "version swap". If there are Da Vinci Clients, they will ingest and swap on their
  own, and the child controller of that region will wait until all DVC instances have swapped (started serving) before
  enacting the region-level swap, after which the servers will also start serving the new store-version to clients which
  perform remote queries. That is why the "DVC Read Traffic SERVING" step is a dependency for the "Server Read Traffic
  SERVING" step.

## Problem Statement 

Pushing to all regions in parallel makes the push faster, but it also means that if the push causes an issue, the impact
is going to be global (affecting all regions). It would be desirable to have certain checks and balances that reduce the
blast radius in cases where the content of the push causes issues. Examples of such issues include:

* Data quality issues:
  * Incomplete data (due to issues in data generation logic, or in upstream datasets).
  * Change in semantics (e.g. some embeddings trained by a new ML model / weights / params / etc. are incompatible with 
    that used at inference-time).
* Schema-related issues:
  * Some optional fields which used to always be populated get deleted (or get populated with null) and the reading app 
    fails due to lack of null checking. This kind of app-side bug can happen even though a schema evolution is fully 
    compatible.
* Infra issues:
  * Larger payloads take more resources, resulting in lack of capacity and thus latency degradation.
  * Certain types of yet-unknown infra bugs are somehow triggered by a data push.

Tighter control of how the new version of the dataset is deployed to each region could allow us to catch issues while
only one region is affected, and abort deploying to other regions. See Scope, below, for specific examples of flow 
control strategies.

In addition, we would like to make it easier to integrate the push job into proprietary monitoring systems such that 
each region getting data deployed to it results in events getting emitted or other observability actions.

## Scope

This proposal is about full push jobs. Incremental pushes and nearline writes are out of scope. At the time of 
submitting this proposal, it is undetermined whether this work will apply to stream reprocessing jobs. In terms of 
priority, we care mostly about supporting full pushes from offline grids, and it may be fine to leave stream 
reprocessing out of scope, although depending on the design details we choose, we may be able to support stream 
reprocessing "for free" as well (i.e. if the hooks are executed in the controller). Incremental Push is out of scope of
this proposal.

The goal is for lifecycle hooks to achieve the following use cases:

* Orchestrate how data is served in each region, including:
  * Ensuring a minimum delay (e.g. 1 hour) between each region beginning to serve the new store-version.
  * Delaying the swapping of a new store-version to be within some time of day (e.g. during "business hours").
  * Performing custom health checks on the client applications to ensure that their key metrics are still healthy within
    a region where a new store-version was swapped, before proceeding to more regions. Based on the outcome of this 
    check:
    * Having the ability to abort the swapping of a store-version to further regions.
    * Having the ability to rollback to the previous store-version in regions that already swapped.
* Trigger informational events in proprietary monitoring systems after important lifecycle milestones are completed.

The above use cases all are operator-centric, and so (at least for now) there is no concern of making it very ergonomic
for Venice users to register new hooks or evolve old hooks dynamically. The general expectation is that there would be
a small number of hooks maintained by the Venice operators and that it's ok for hooks to be bundled and upgraded 
alongside the Venice components. In cases where Venice users need to customize hook behaviors, that could be achieved
via store-level configs passed into hooks, and there is no need to provide the flexibility of letting users register
whole new hook implementations.

## Project Justification

The cost of having global impact in the case of issues mentioned above is too high, and we would like to provide 
first-class options to reduce the blast radius. Building this within Venice itself will make it easier to automate these
methodologies, thus reducing toil for users.

## Functional specification

The proposed API for this functionality is described in code here:

[//]: # (Got to remove the /venice prefix before pushing to main on linkedin/venice, otherwise links will be broken...)

* [StoreLifecycleHooks](/venice/javadoc/com/linkedin/venice/hooks/StoreLifecycleHooks.html), which is the main part of this 
  proposal.
* [StoreLifecycleEventOutcome](/venice/javadoc/com/linkedin/venice/hooks/StoreLifecycleEventOutcome.html), which is the signal
  returned by some hooks to indicate that a given step should proceed or abort.
* [StoreVersionLifecycleEventOutcome](/venice/javadoc/com/linkedin/venice/hooks/StoreVersionLifecycleEventOutcome.html), which
  is the signal returned by some other hooks which need more fine-grained control over the workflow. In addition to 
  proceeding and aborting, this also provides the option to wait, which tells the hooks framework to try invoking the 
  hook again later, and rollback, which tells the framework to rollback to the previous store-version in all regions.
* [JobStatusQueryResponse](/venice/javadoc/com/linkedin/venice/controllerapi/JobStatusQueryResponse.html), which is the payload
  returned by the `/job` controller endpoint, is extended to include status update timestamps. This will be populated by
  the child controller to indicate the time when its own individual status last changed, and the parent controller will
  aggregate these into a map keyed by region. All hooks which return the `StoreVersionLifecycleEventOutcome` will have
  access to this payload in their input, so that they can make decisions based on the status of each region. The time
  when the status was last updated for a given region is useful in order to achieve the use case of a hook which injects
  a delay between each region swap. The code change to support these extra timestamps is included in this VIP, to 
  demonstrate feasibility and present the proposed algorithm (see `OfflinePushStatus::getStatusUpdateTimestamp`).

## Proposed Design

The main design consideration is where to execute the hooks. At a high level, there are three options:

1. Within the push job.
2. Within the parent controller (chosen option).
3. Within the child controllers.

It is also possible to consider invoking some hooks in one of these location while other hooks would be executed 
elsewhere. The table below summarizes the feasibility and tradeoffs for each of the proposed hooks:

|                           Hook function name |  Actions   | CC  | PC  | VPJ |
|---------------------------------------------:|:----------:|:---:|:---:|:---:|
|                         `validateHookConfig` |   ➡️ ☠️    |  ❌  |  ✅  |  ❌  |
|                          `preStartOfPushJob` |   ➡️ ☠️    |  ❌  |  ✅  |  ✅  |
|                         `postStartOfPushJob` |    None    |  ❌  |  ✅  |  ✅  |
|                      `preSchemaRegistration` |   ➡️ ☠️    |  ❌  |  ✅  | 1️⃣ |
|                     `postSchemaRegistration` |    None    |  ✅  |  ✅  | 1️⃣ |
|                    `preStoreVersionCreation` | ➡️ ☠️ ✋ ↩️ |  ✅  |  ✅  |  ✅  |
|                   `postStoreVersionCreation` |    None    |  ✅  |  ✅  |  ✅  |
|  `preStartOfStoreVersionIngestionForDaVinci` | ➡️ ☠️ ✋ ↩️ | 2️⃣ | 2️⃣ | 2️⃣ |
| `postStartOfStoreVersionIngestionForDaVinci` | ➡️ ☠️ ✋ ↩️ | 2️⃣ | 2️⃣ | 2️⃣ |
|          `postStoreVersionLeaderReplication` |    None    |  ✅  |  ❌  |  ❌  |
|                        `preStoreVersionSwap` | ➡️ ☠️ ✋ ↩️ |  ✅  |  ✅  |  ✅  |
|                       `postStoreVersionSwap` | ➡️ ☠️ ✋ ↩️ |  ✅  |  ✅  |  ✅  |
|                            `preEndOfPushJob` | ➡️ ☠️ ✋ ↩️ |  ✅  |  ✅  |  ✅  |
|                           `postEndOfPushJob` |    None    |  ✅  |  ✅  |  ✅  |

**Legend:**

* The Actions column represent which control mechanism is available to each hook:
  * ➡️ Proceed: Move forward with this step.
  * ☠️ Abort: Cancel this step (and as a consequence, short-circuit any future step that would come after this one).
  * ✋ Wait: Let the hooks framework re-run this step later (i.e. 1 minute later, by default).
  * ↩️ Rollback: Let the store go back to the store-version it had prior to beginning the push job (in all regions). 

* The last three columns are the feasibility of implementing this hook in a given component:
  * **CC** Child Controller.
  * **PC** Parent Controller.
  * **VPJ** Venice Push Job.
  * ✅ It is feasible to implement this hook within this component (without unreasonable complexity).
  * ❌ It is NOT feasible to implement this hook within this component (without unreasonable complexity).
  * 1️⃣ Schema registration hooks in push jobs could only be invoked in cases where auto-registration is enabled and 
  the new schema originates from the push job itself, whereas schema registrations which are performed directly on the 
  controller could not trigger the hook.
  * 2️⃣ The hook for the start of ingestion for Da Vinci Clients is tricky for a few reasons. The start of ingestion is 
  controlled by updating the Meta Store, which is a non-replicated system store updated by child controllers, so those
  must be involved (either by running the hook there in the first place, or by having some mechanism that enables the
  parent or VPJ to inform the child controllers of when the system store is eligible for getting updated, such as by
  adding a new field to the `AddVersion` admin channel command). However, see Rollback Support below for why running 
  hooks in the child controller may be insufficient.

### Rollback Support

In order to support the ability for the hooks which return the `StoreVersionLifecycleEventOutcome` to rollback, the most
natural way to achieve this is likely to involve the parent controller, either by having those hooks run there in the 
first place, or by having a propagation mechanism to it:

* If the hooks are executed in the child controller, the propagation mechanism might be to extend the job status check 
  which the parent controller does periodically in order for the child to inform the parent of the need to rollback, or 
  else build a new mechanism for the child to directly interact with the parent (or even with other child controllers 
  directly...).

* If the hooks are executed in VPJ, then it would need to interact with the parent via the controller client to trigger 
  the rollback.

### Configs

There needs to be new configs:

* `venice.store.lifecycle.hooks`: Comma-separated list of FQCN of the hook implementations to load.

* `venice.store.lifecycle.hooks.threads`: Number of threads used by the hooks framework to execute all hooks.

* `venice.store.lifecycle.hooks.timeout.seconds`: Max duration allowed for a hook before the hooks framework interrupts
  it.

* `venice.store.lifecycle.hooks.wait.interval.seconds`: The time to wait before re-invoking a hook which returned `WAIT` 
  (default: 60 seconds).

* `venice.store.lifecycle.hooks.configs.<arbitrary>`: Any number of configs to be passed (after clipping everything
  before the `<arbitrary>` part) into the hooks constructor.

In addition, the store config will get a new `Map<String, String>` of store-level config overrides. Those configs are
"[stringly-typed](https://wiki.c2.com/?StringlyTyped)", rather than strongly-typed, since we are not aware of the 
configs needed by each hook at compile-time, and we therefore cannot shape the definition of the store config schema 
accordingly. This issue is mitigated via the `validateHookConfig`, which can be used to prevent invalid configs from
entering the system.

### Metrics

There needs to be new metrics to monitor hook health. Each new metric will be per function and per registered hooks 
class (i.e. 13 functions per class if we implement all of them, or less if we cut the scope). For each class/function
hook, there will be:

* the occurrence rate of:
  * hook invocations
  * each hook return signal (for the functions that return something other than `void`)
  * failures (exceptions)
  * timeouts
* the time spend waiting in queue before being executed (which will be useful to determine if the thread pool count is 
  under-provisioned)

N.B.: Implementing metrics from within VPJ is a bit more complicated, whereas controllers already have the ability to
emit metrics.

### Design Recommendation

Running hooks in the parent controller is probably most straightforward. The only issue is the inability to support the
`postStoreVersionLeaderReplication` hook, but that one is not critical and could be left out of scope.

Concretely, choosing the parent controller path would work like this:

1. The parent controller would invoke the hooks for `preStartOfStoreVersionIngestionForDaVinci` and for
   `preStoreVersionCreation` for each of the regions. If all hooks respond with `PROCEED` then it's essentially a normal 
   push, otherwise it would configure the `AddVersion` command sent to the admin channel with the appropriate inclusions
   in `targetedRegions` (depending on the result of `preStoreVersionCreation`) and in a new field for controlling the
   DVC ingestion (depending on the result of `preStartOfStoreVersionIngestionForDaVinci`) which the child controller 
   would honor by holding off on writing to the Meta Store.

2. The parent controller would create the new store-version with `versionSwapDeferred = true`.

3. When the parent controller sees that a region has completed ingestion, it would invoke the `preStoreVersionSwap`
   hook for that region, and if the hook responds with `PROCEED`, then it would send an admin command targeted for that
   region to swap the current version.

4. If at any stage the hooks respond with `ROLLBACK` then the parent controller would send more admin channel commands
   to do the swap in the reverse direction.

**This recommendation has been accepted, after design reviews, hence we will implement hooks in the parent controller.**

## Development Milestones

At a high-level:

1. Evolve the admin channel protocol.
2. Implement the child controller changes for dealing with the admin channel changes.
3. Implement the parent controller changes to support the hooks framework and the orchestration described above.

More fine-grained plan TBD after finalizing the design.

## Test Plan

The first hooks will be built such that they are no-op by default, and require a hook config to enable them. That hook
config will be left disabled in the global config, and will be tested at small scale via the store-level overrides.

After store-level testing and stabilization is satisfactory, we will begin enabling them globally.

## References 

N/A.