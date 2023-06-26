---
layout: default
title: [VIP-2] Removing Per Record Offset Metadata From Venice-Server Storage With Heartbeats
parent: Community Guides
permalink: /docs/proposals/VIP_TEMPLATE.md
---

# [VIP-2] Removing Per Record Offset Metadata From Venice-Server Storage With Heartbeats

* **Status**: Discussion
* **Author(s)**: Zac Policzer
* **Pull Request**: _TODO_
* **Release**: _TODO_



## Introduction

This VIP explores a strategy for removing the offset metadata stored per record in Venice by utilizing replica heartbeats. This VIP also explores what possible other benefits might be had by introducing heartbeats, and why it would be a strategic feature to adopt.

## Problem Statement

Today, Venice stores which have Active/Active replication enabled store N number of offsets per record where N is the number of replicating sites. Venice does this in order to enable data drift detection between active sites via a Venice algorithm called Leap Frog (which we will document in a later section of this VIP and accompanying PR). At a high level, Leap Frog enables Venice operators to detect and flag inconsistencies triggered by outages and bugs with very high granularity.

In addition, these per record offsets are utilized by Venice Change Capture consumers for filtering out duplicated events across version pushes, so that downstream consumers do not get callbacks triggered for events that they already consumed.

This was a fine approach, however, it presumes that for a given message in an underlying pubsub implementation, that the object used to describe it's position (in Kafka's case, a long to represent an offset) is reasonably small. So long as it's small, the added overhead per record is tolerable.

This is not neceassrily the case. **In at least one PubSub implementation that Venice will need to support, the position object is as large as over 20 Bytes.**

Venice (at least in LinkedIn deploments at time of writing) is a storage bound service, and this high overhead has real cost and performance implications.

## Scope

1. **What is in scope?**

   We must be able to shrink down or remove this per record overhead in the venice server, but still be able to retain the functionality that we depend on (diff detection and change capture client filtering).

2. **What is out of scope?**

    We will not seek to reduce the footprint of this data in the underlying PubSub (where it's presumed that disk is cheaper).

## Project Justification

This project seeks to improve the **performance, scalability, and cost to serve of Venice**. By shrinking the per record metadata we'll not only save on disk, but we should be able to improve the ingestion speed slightly (by removing some per record overhead) and reduce the potential increase in storage cost as new replicating sites (henceforth refered to as **colos**) are added.

## Proposed Design

Before we can discuss proposed designs, it's important to clear up some aspects of per record pubsub positions (henceforth referred to as **offsets**) which have heretofore not been documented in Venice open source (but for which there are some mentions of in the code).

### How Are Per Record Offsets Used Today?
#### **Leap Frog**

Leap Frog works on the notion that we can detect if two replicas diverge for a record if the following holds true when comparing replica A and B

1. The value for a certain key on replicas A and B are different AND
2. The record on replica A was received from an offset that falls below a highwatermark of applied events on replica B AND
3. The value on replica B was received from an offset that falls below a highwatermark of applied events on replica A

If all three are true, then we can detect divergence.  The implementation of LeapFrog is based on the consumption of a Venice version topic (as opposed to interrogating the on disk state of Venice).

We won't deep dive Leap Frog here, but in order to give the reader more context, there is [a TLA+ spec included with this PR](../../specs/TLA%2B/LeapFrog/leapfrog.tla) which explains and and provides some validation of the correctness of the detection algorithm.

#### **Venice change capture**

Venice change capture relies on per record metadata in order for the client to be able to screen out duplicate events.  Duplicated events either come in the form of duplicate events sent from the PubSub Broker, or events which are applied following a rewind on a version push.  It works by maintaining a high watermark state on the client which screens out events which fall below the cached high watermark.

#### How are offsets stored

When a record is consumed from a given colo's RT, the offset position that it was consumed is noted and merged into a vector of offsets stored alongside that record.  The vector is a set of high watermark offsets consumed from each replicating site. So if we have three sites actively replicating a given partition, we'll store at most three offsets per record. We store this because of the way Venice handles write compute and AA.  The state of the record can be the accumulated state of disjoint updates from different colos, and this is why it's not sufficient to store just the LAST update to a given record.

### What are the base common requirements

If you squint at both of these, they boil down to relying on the following properties.

* Being able to build a high watermark based on events consumed
* Being able to causally compare a consumed event/record to this highwatermark and take action.

We're able to meet the first requirement by just starting at an arbitrary point in a version topic stream and consuming.  Every single event has an RMD field that can help a consumer of this data build a highwatermark.  And then since every single record we consume has the same information, it's very straightforward to determine the causality of that event relative to a built up high watermark.

That said, it's not actually a requirement to be able to do this on every single event we consume. It's possible to meet the first two requirements at a courser granularity of updates.

### Heartbeat Algorithm

Here we introduce the notion of heartbeats as a means of checkpointing the stream. Today a stream of events looks like this:

>Record 1: {1, 200, 5000}
>
>Record 2: {0, 201, 4000}
>
>Record 3: {5, 195, 3000}
>
>Record 4: {3, 205, 5001}

From the above, if we consumed events 1-3 we'd construct a high water mark of {5, 201, 5000}, and if we looked at Record 4, we could see that it comes after our highwatermark as it's middle component vector (205) is higher then the middle component vector of our high water mark (which is at 201).

Now, lets see if we can try and come to a similar conclusion with minimal information. **minimal infromation** means that we only have access to which active site and at what offset this event was triggered by in a colo's upstream real time topic.  This is essentially all the information we have on hand after ingesting an event in the server.  If we publish just that information into the version topic, we would have an event stream which looked like:

>Record 1: {_, 200, _}
>
>Record 2: {_, 201, _}
>
>Record 3: {5, _, _}
>
>Record 4: {_, _, 5001}

If we consume events 1-3, we'll build a high water mark of {5, 201, _} and when consuming record 4, it's very hard to actually determine if this record actually comes from before or after our high water mark. We don't have enough information about the current state to definitively come to that conclusion.

So lets see if we can make this work with heartbeats.  The original issue with the aforementioned scenario was that we didn't have enough information about our third active site in order to make a call on the causality of Record 4.

>HEARTBEAT: {_, _, 4500}
>
>Record 1: {_, 200, _}
>
>Record 2: {_, 201, _}
>
>Record 3: {5, _, _}
>
>Record 4: {_, _, 5001}

Now, if we consume that heartbeat and read up to record 3, we'll have a high water mark of {5, 201, 4500}, and when looking at record 4, we can see that it has a vector component that is higher then our high water mark.

The reason why the above works is because we're able to take for granted that we'll at some point either in the past or future get at least one update to help us build our high water mark.

Now, the presented example is somewhat simplistic and there are some edge cases.  For example, what if our sequence of events was with a slightly different heartbeat. Something like:

>HEARTBEAT: {_, 199, _}
>
>Record 1: {_, 200, _}
>
>Record 2: {_, 201, _}
>
>Record 3: {5, _, _}
>
>Record 4: {_, _, 5001}

This is problematic. If reading from the heartbeat up to record 3, then we're stuck in the same situation as the minimal data example. So this informs that we need to be able to adhere to a property where when doing this kind of evaluation, we're able to assemble some information about all replicating colos. We can avoid this in all cases by making sure the heartbeat has all potentially revelevant information.  This would now look like:

>HEARTBEAT: {3, 199, 4500}
>
>Record 1: {_, 200, _}
>
>Record 2: {_, 201, _}
>
>Record 3: {5, _, _}
>
>Record 4: {_, _, 5001}

The heartbeat is now a published baseline that we can use to build our highwatermark when doing the comparison.

**This approach is advantageous because it means we no longer have to merge together offset vectors in per row RMD's on venice servers. We need only have to publish on hand information to the version topic for a given store partiiton**

#### Leap Frog with heartbeats (two colos)

Lets look at what this implies for Leap Frog.  Leap Frog compares a high water mark and a record that both are remote from each other.

>**COLO A**
>
>HEARTBEAT: {3, 199, 4500}
>                           
>Record 1: {_, 200, _}
>
>Record 2: {_, 201, _}
>
>Record 3: {5, _, _}
>
>Record 4: {_, _, 5001}

>**COLO B**
>
>HEARTBEAT: {4, 199, 4900}
>                           
>Record 1: {_, 200, _}
>
>Record 2: {_, _, 4950}
>
>Record 3: {_, 300, _}
>
>Record 4: {20, _, _}

Originally, we only needed the highwatermark of coloA and the rmd of an individual record in coloB (and vice versa going the other way). However, in this context, we need more information. Each record will only have the offset of the LAST upsream RT message which touched the row.  We can backfill a bit more information based on the last heartbeat. Since everything is produced in serial in increasing offsets we know that the other updates came from an offset previous to the published highwatermark in the last received heartbeat.  So, this looks like:

>**COLO A**
>
>HEARTBEAT: {3, 199, 4500}
>                           
>Record 1: {<3, 200, <4500}
>
>Record 2: {<3, 201, <4500}
>
>Record 3: {5, <199, <4500}
>
>Record 4: {<3, <199, 5001}

>**COLO B**
>
>HEARTBEAT: {4, 199, 4900}
>                           
>Record 1: {<4, 200, <4900}
>
>Record 2: {<4, <199, 4950}
>
>Record 3: {<4, 300, <4900}
>
>Record 4: {20, <199, <4900}

Now this starts to look a bit more like what we have today, just at a courser granularity.  This has advantages and disadvantages.  One nice advantage is that heartbeat records can be used as logical markers for chunks of the version topic stream which can be directly compared (keys which have value mismatch or don't appear within comparable ranges are either divergent or completely missing).

There is a downside however.  Previously, we could detect divergence at a very fine granularity (essentially, at every write). In previous testing, this was valuable because in order to trigger some bugs, we had to write very aggressively to a small keyspace, so it was useful to be able to detect and flag this divergence at a situation where the divergence only existed for less then a second. **With the above approach, it is actually only possible to detect when records diverge based on the compacted view of records between heartbeats**.

### Other Advantages To Be Had From Heartbeats

Heartbeats are something that we've talked about internally in a few contexts. Some possible advantages that come to mind:

* **Time Based Checkpointing Ecosystem:**
If a heartbeat were to also carry with it some notion of time, we could use it to checkpoint the upstream RT with all systems which follow.  Verion topics, change capture topics, views, and ETL's can all be checkpointed based on these upstream heartbeats and ca now be tied together in a coherent way.

* **Transmit and Restore DIV State:**
If a leader were broadcasting their state with each heartbeat message to downstream consumers like followers via VT, then DIV state could be restored should a Leader node go offline. This would clean up DIV error false positives (or negatives?) potentially triggered from follower rewind.

* **Proactive Fault Detection:**
Today, our lag monitoring relies on users transmitting writes to Venice. If a fault has ocurred and no one is transmitting data, we are none the wiser. Heartbeats coupled with adequate metric emission on the time since last received heartbeat would help us detect these problems before they become a problem for end users.

### Implementation

**THIS IS SECTION IS WIP**

#### **Leaders Emit Heartbeats**
The suggestion here is that for every leader for every store partition, the leader emits a heartbeat control message directly to it's local RT.  It then consumes this message, and publishes another control message into local VT, which will then be consumed by followers.  Meanwhile, other leaders in remote colo's also consume the remote RT event and do the same.  The idea here is that by emitting directly to the top of the replication pipeline, all parties which participate in consuming this stream of data (PubSub broker, Leader, Followers, remote nodes, clients, etc.) can all individually give testimony to being in working order.  Leaders upon receiving the message publish relevant metadata in the form of their local upstream RT highwatermarks, and DIV/timestamp information.

### **Other Ideas (RFC's)**









