---
layout: default
title: VIP-1
parent: Community Guides
permalink: /docs/proposals/VIP_TEMPLATE.md
---

# [VIP-2] Removing Per Record Offset Metadata From Venice-Server Storage

* **Status**: Discussion
* **Author(s)**: Zac Policzer
* **Pull Request**: _TODO_
* **Release**: _TODO_



## Introduction

This VIP explores strategies for removing the offset metadata stored per record by utilizing heartbeats as well as an exploration of what other system benefits can be had by adopting replica heartbeats.

## Problem Statement

Today, Venice stores which have Active/Active replication enabled store N number of offsets per record where N is the number of replicating sites. This is done so as to enable data drift detection via a Venice algorithm called Leap Frog (which we will document in a later section) so that it's possible to detect and flag inconsistencies triggered by outages and bugs with very high granularity.

In addition, these offsets are utilized by Venice Change Capture consumers for filtering out duplicated events across version pushes, so that downstream consumers do not get callbacks triggered for events that they already consumed.

This was a fine approach, however, it presumes that for a given message in an underlying pubsub implementation, that the object used to describe it's position (in Kafka's case, a long) is reasonably small. So long as it's small, the added overhead per record is tolerable.

This is not neceassrily the case. **In at least one PubSub implementation that Venice will need to support, the position object is as large as over 20 Bytes.**

Venice (at least in LinkedIn deploments at time of writing) is a storage bound service, and this high overhead has real cost implications.

## Scope

_Call out explicitly:_

1. **What is in scope?**

   We must be able to shrink down or remove this per record overhead in the venice server, but still be able to retain the functionality that we depend on (diff detection and change capture client filtering).

2. **What is out of scope?**

    We will not seek to reduce the footprint of this data in the underlying PubSub (where it's presumed that disk is cheaper).

## Project Justification

This project seeks to improve the **performance, scalability, and cost to serve of Venice**. By shrinking the per record metadata we'll not only save on disk, but we should be able to improve the ingestion speed slightly (less bytes is less bytes afterall) and reduce the potential increase in storage cost as new replicating sites (henceforth refered to as **colos**) are added.

## Proposed Design

Before we can discuss proposed designs, it's important to clear up some aspects of per record pubsub positions (henceforth referred to as **offsets**) which have heretofore not been documented in Venice open source (but for which there are some mentions of in the code).

### How Are Per Record Offsets Used Today?
* #### **Leap Frog**

* #### **Venice Change Capture**

_This section must describe the proposed design and implementation in detail, and how it
addresses the problems outlined in the problem statement. It must provide details on alternative solutions
that were considered, stating clearly why they lost out to the final design. This latter point is particularly
important or else we tend to vacillate on old decisions because we cannot remember the original rationale.
Phrase this section in terms of trade offs since it is rare that one approach is better in all ways; there is
generally a tradeoff and making that explicit makes it much easier to understand._

_The following aspects of the design must be covered when applicable:_

1. _Changes to APIs or protocols or data format highlighting any compatibility issues and how they will be addressed._
2. _Major components and sequence diagrams between them._
3. _Multi-region implications including Parent/Child Controller communication._
4. _Alternative designs considered, and brief explanation of tradeoffs and decision versus the chosen design,
and important aspects to consider_:
   1. Security.
   2. Performance.
   3. Maintainability.
   4. Operability.
   5. Scalability.
   6. Compatibility.
   7. Testability.
   8. Risks/Mitigations.
   9. Overall effort.
   10. Leverageable.
   11. Align with the long-term vision.
5. Preferred option by the designer/author.
   1. _Conclusions/Decisions made in the design review sessions. Explanation why it’s selected_

## Development Milestones
_This section described milestones for rather large projects so that we can set up intermediate goals for
better progress management.
For small projects, this may not be necessary._

## Test Plan

_Describe in few sentences how the functionality will be tested.
 How will we know that the implementation works as expected? How will we know nothing broke?_

1. _What unit tests would be added to cover the critical logic?_
2. _What integration tests would be added to cover major components, if they cannot be tested by unit tests?_
3. _Any tests to cover performance or Security concerns._


## References

_List any references here._

## Appendix

_Add supporting information here, such as the results of performance studies and back-of-the-envelope calculations.
Organize it however you want but make sure it is comprehensible to your readers._









