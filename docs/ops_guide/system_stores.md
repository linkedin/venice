---
layout: default
title: System Stores
parent: Operator Guides
permalink: /docs/ops_guide/system_stores
---

# System Stores

Venice has several "system stores". These are used to store data and metadata needed by the system itself, in opposition 
to "user stores" which store the data users are interested in. For the most part, the existence of these system stores 
should not matter, but it can be useful for operators to know about them.

This page explains the various types of system stores and their function.

## System Store Types

Below are a few distinct types of system stores:

| Type                   | Cardinality    | Content       |
|------------------------|----------------|---------------|
| Schema system store    | 1 / deployment | Metadata only |
| Global system store    | 1 / deployment | Data          |
| Cluster system store   | 1 / cluster    | Data          |
| Per-store system store | 1 / user store | Data          |

### Schema System Stores

These are stores created in just one of the clusters of a deployment. They contain no data at all (and therefore no 
store-versions) and thus cannot be queried. They only serve to store schemas. These schemas are those which the system
needs for its own [internal protocols](https://venicedb.org/javadoc/com/linkedin/venice/serialization/avro/AvroProtocolDefinition.html).

The schema system stores are used for the sake of protocol forward compatibility. When deploying Venice code, its jars 
contain resource files for the current protocol versions as well as all previous protocol versions, but not future 
protocol versions. Given that Venice is a distributed system, it is important to have the flexibility of deploying 
different versions of the software across different nodes. When a node running a newer version of the software persists
data written with a newer protocol version, and that data is then read by another node running an older version of the
software which does not know about that newer protocol version, then it can query the schema of the appropriate system
store and use this to decode the newer payload, and translate them into the version they understand (a process known as
Schema Evolution).

### Global System Stores

These are stores created in just one of the clusters of a deployment, but in opposition to schema system stores, they
do contain data. Their global nature makes them convenient to store data about all clusters, but it also means that the
clusters are not isolated from one another as far as this kind of system store is concerned. Therefore, it is best for
this type of system store to serve only in "best effort" purposes, so that if for any reason they become unavailable,
critical operations can still continue. An example of this would be to store heuristics or other non-critical data.
These include:

- Push Job Details Store
- Batch Job Heartbeat Store

### Cluster System Stores

These are stores created once per cluster, and which do contain data. It is appropriate to use such stores for critical
operations, given that one cluster's availability issues would not bleed into other clusters. These include:

- Participant Store

### Per-store System Stores

These are stores created once per user store, and which do contain data. These include:

- Da Vinci Push Status Store
- Meta Store

## System Store List

Below is a list of actual system stores, along with their type and function (excluding schema stores):

| Name                       | Type      | Function                                                                                                                                                                                                     |
|----------------------------|-----------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Push Job Details Store     | Global    | Stores historical status and characteristics about all [Push Jobs](../user_guide/write_api/push_job.md) that ever ran.                                                                                       |
| Batch Job Heartbeat Store  | Global    | Stores heatbeats emitted from running push jobs, in order to detect if any push job terminated abruptly without cleaning itself up.                                                                          |
| Participant Store          | Cluster   | Intended as a signaling mechanism for commands emitted by controllers, to be executed by servers, including kill job commands.                                                                               |
| Da Vinci Push Status Store | Per-store | Stores per-instance consumption status of [Da Vinci Clients](../user_guide/read_api/da_vinci_client.md). Written to by DVC, and read by controllers, in order to determine whether push jobs have succeeded. |
| Meta Store                 | Per-store | Stores a copy of the store config (also found in Zookeeper). Written to by controllers, and read by DVC. Deprecated, will be removed eventually.                                                             |
