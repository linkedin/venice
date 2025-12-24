---
layout: default
title: VIP Template # Replace with: VIP-{VIP_Number} {VIP_Title}
parent: Proposals
permalink: /docs/proposals/vip_template # Replace with: /docs/proposals/vip-{VIP_Number}
---

# VIP-`$VIP_Number`: `$VIP_Title`

* **Status**: _"one of ['Under Discussion', 'Accepted', 'Implemented', 'Rejected']"_
* **Author(s)**: _(Names)_
* **Pull Request**: _(Link to the main pull request to resolve this VIP)_
* **Release**: _(Which release include this VIP)_

_Please make a copy of this page - DO NOT EDIT this design document directly, unless you are making 
changes to the template._

_Remove the instructions (in italics) before publishing._

## Introduction  _(200 words at most)_

_This section should provide a concise and comprehensive overview of this document. After reading the introduction, 
the reader should have a high level understanding of the problem being solved (the "what"), why the problem is 
important (the "why"), and how this design intends to solve it (the "how"). 
When the draft design document is circulated, readers may use the introduction to determine if it is 
applicable to their area of work and if they are interested in reading further._

## Problem Statement 

_This section should frame the problem clearly and state the non-negotiable requirements for a solution. 
There is some overlap with the Introduction section but it is here for emphasis since it forms the basis 
for evaluating any proposed design. There should be no disagreement on the problem statement, 
otherwise a design or implementation is unlikely to be successful._ 


## Scope

_Call out explicitly:_

1. **What is in scope?**

   _For example, this functionality will only apply to batch-only stores._

2. **What is out of scope?**

    _For example, this functionality won’t apply to DaVinci Clients._

## Project Justification

_This section should justify the necessity to address the problem mentioned above, in another way, 
we could think about what will happen if we don’t solve the problem, and we could consider the following dimensions._
1. New use cases.
2. Customer experience
3. Productivity improvement.
4. Resilience.
5. Operability improvement.
6. Performance improvement.
7. Maintainability improvement.
8. Scalability improvement.
9. Cost to Serve reduction.
10. Reduce Toil.
11. Craftsmanship improvement.
12. Security improvement.
13. Etc.

## Functional Specification

_If this is a development of a functionality/product for users, specify what it should look like from the user's point of view.
It may include:_
1. Public API.
2. Expected public behavior.
3. Public behavior of error handling, etc.

## Proposed Design

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









