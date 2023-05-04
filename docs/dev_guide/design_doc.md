---
layout: default
title: Design Documents
parent: Developer Guides
permalink: /docs/dev_guide/design_doc
---
# Design Documents

## Venice Improvement Proposal (VIP)

Venice follow the Venice Improvement Proposal (VIP). VIP is the mechanism used to propose changes to the 
Venice Codebase. 

Not all the changes require a VIP treatment, Please use your commonsense to check if writing this VIP help yourself 
and the reviewers time instead of just doing it in the pull request itself. Generally, this applies to
* Large code refactors
* New functionality 
* Public API changes

In practical terms, the VIP defines a process in which developers can submit a design doc, 
receive feedback and get the "go ahead" to execute.

## Who can create a VIP?

Any person willing to contribute to the Venice project is welcome to
create a VIP.

## How does the VIP process work?

A VIP proposal can be in these states:
1. **DRAFT**: (Optional) This might be used for contributors to collaborate and
   to seek feedback on an incomplete version of the proposal.

2. **DISCUSSION**: The proposal has been submitted to the community for
   feedback and approval.

3. **ACCEPTED**: The proposal has been accepted by the Venice project.

4. **REJECTED**: The proposal has not been accepted by the Venice project.

5. **IMPLEMENTED**: The implementation of the proposed changes have been
   completed and everything has been merged.

5. **RELEASED**: The proposed changes have been included in an official
   Venice release.


The process works in the following way:

1. The author(s) of the proposal will create a file named "VIP-xxx.md" in [proposal](../../docs/proposals) folder cloning 
    from the [template for VIP proposals](../../docs/proposals/VIP_TEMPLATE.md). The "xxx" number should be chosen to 
    be the next number from the existing VIP issues, listed [here](../../docs/proposals/)
2. The author(s) submit this file as a PR named "VIP-xxx: {short description}".
3. People discuss using PR comments, each is its own threaded comment. 
   General comments can be made as general comment in the PR. There are two ways for an interactive
   discussion. 
   1. Venice Community [Slack Channel](https://communityinviter.com/apps/venicedb/venice) 
   2. Venice Contributor Sync Meeting, see details [here](../CONTRIBUTING.md) at Contributor Sync Meeting
4. Comment was done? They resolve it. This way you see what the pending discussions are at a glance.
5. VIP comments resolved or no more concerns? Awesome. A Venice maintainer will merge the PR. 
    Merge == Approved 
6. Rejected ? Close the PR 
    Closed == Rejected. 
7. All Pull Requests for this functionality, should prefix this "VIP-XXX" number in the title for quick access. 

## Acknowledgements

This guide is inspired from the 
[Apache Pulsar project proposal](https://github.com/apache/pulsar/blob/master/wiki/proposals/VIP.md) plan. 




