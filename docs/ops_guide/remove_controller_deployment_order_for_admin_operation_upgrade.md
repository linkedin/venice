---
layout: default
title: Remove Controller Deployment Order for Admin Operation Upgrade
parent: Operator Guides
permalink: /docs/ops_guide/remove_controller_deployment_order_for_admin_operation_upgrade
---

# Remove Controller Deployment Order for Admin Operation Upgrade
This document outlines a new approach to deploy admin operation protocol upgrades in the Venice controller.
Previously, strict deployment order was required to prevent deserialization failures.
The solution introduces a ZooKeeper (ZK)-based configuration to manage serialization compatibility across the cluster.
A Version Detection Service ensures all controllers use a compatible protocol version, while a Semantic Detector prevents bad message leakages from unsupported admin operations into the kafka Admin Topic.

# Problem Statement
Previously, strict deployment order between parent and child controllers was required to avoid issues with protocol upgrades. 
Parent controllers could produce messages that child controllers couldn't deserialize. This issue applies for any protocol upgrade.
With the scope of this document, we will focus on the admin operation protocol upgrade.

Two common scenarios are:
1. Unknown writer schema
- If parent controllers deploy first and produce an admin message with a new schema, child controllers do not have the writer schema to deserialize the admin message. Admin consumption will be blocked.
- If we do canary testing for parent controllers, this parent controller starts producing new messages. In this case, when the leader gets rebalanced to a different node with older protocol, the newly selected leader wouldn’t understand the message.
2. Unknown union type
- Admin Operation is union type. If parent controllers deploy first and produce a new admin operation, even when the child controller can deserialize the job, it would be an unknown admin operation type. The new admin operation will fail.

# High-level Solution
Introduce a ZooKeeper (ZK) configuration to standardize serialization. The system selects a “GOOD” version—the lowest supported version across all consumer-controllers—to ensure compatibility. 
A detection service periodically evaluates consumer versions, updating ZK when necessary.

Key Design Updates:

- **New Configuration**: `adminOperationProtocolVersion` added to cluster-level ZK.
- **Version Detection Service**: Runs periodically to identify the lowest compatible version and updates ZK.
- **New Semantic Detector**: Ensures unsupported admin operations fail fast instead of being processed incorrectly. Checks differences between schemas and flags invalid usage.

# Solution Design
## New Configuration
A new configuration, `adminOperationProtocolVersion`, will be introduced in `adminTopicMetadata` at the cluster level in ZooKeeper (ZK).
Parent controllers will retrieve this version from ZK when serializing admin operation messages for the AdminTopic queue.

## New Version Detection Service
ProtocolVersionAutoDetectionService runs at the cluster level, ensuring all controllers, including parent, child, and standby nodes, remain synchronized on protocol versions.
It shares the lifecycle with HelixVeniceClusterResources, activating when a parent controller becomes a leader for one cluster, and stopping when leadership changes. 
This service periodically checks protocol versions and updates the "GOOD" version in ZK.

Process overview:
1. Leader controllers (both parent and child) request local versions from standby controllers.
2. The leader consolidates all versions into a map, identifying the lowest supported version across all controllers and sending metadata back to leader parent controller.
3. If the "_GOOD_" version changes and is different from the existing ZK entry, it gets updated—unless the stored version is -1 or already matches the detected version.

> What is the "_GOOD_" version? 
> 
> The "_GOOD_" version is the protocol version used by the writer for message serialization, ensuring that all controllers in the cluster can successfully deserialize it.
> It is the lowest protocol version supported by all parent and child controllers (including standby controllers) of specific cluster, to guarantee compatibility across the system.

## New Semantic Detector
NewSemanticDetector ensures unsupported admin operations fail fast instead of being processed incorrectly.

Detection mechanism:
- Compares two schemas as tree structures using TraverseAndValidate (Depth-First Search).
- Flags differences between schemas based on object values.

Identifying New Semantics:
- Fields that exist in the current schema but are **absent** in the target (typically an older) schema.
- Enum fields with additional or changed values.
- Fixed fields with size discrepancies (strictly disallowed).

Bad Semantic Usages (Triggers Failure):
- Default field values differing from expected defaults.
- Enum values introducing unsupported new types.
- Fixed fields with mismatched sizes.
- Objects that do not conform to the current schema.

When a bad semantic usage is detected, the system throws a VeniceProtocolException and fails the request immediately.

# Operational Guide
1. What are configs to be set?
```java
  /**
   * Enables / disables protocol version auto-detection service in parent controller.
   * This service is responsible for detecting the admin operation protocol version to serialize message
   * Default value is disabled (false).
   */
  public static final String CONTROLLER_PROTOCOL_VERSION_AUTO_DETECTION_SERVICE_ENABLED =
      "controller.protocol.version.auto.detection.service.enabled";

  /**
   * Specifies the sleep time for the protocol version auto-detection service between each detection attempt.
   * Default 10 mins
   */
  public static final String CONTROLLER_PROTOCOL_VERSION_AUTO_DETECTION_SLEEP_MS =
      "controller.protocol.version.auto.detection.sleep.ms";
```
2. How to start/stop the version detection service?
- Set `CONTROLLER_PROTOCOL_VERSION_AUTO_DETECTION_SERVICE_ENABLED` to `true` in the parent controller to start the version detection service, 
and `false` to stop it.

3. How to stop auto-updating ZK with detected version?
- Set the upstream version in ZK to `-1` to stop auto-updating ZK with the detected version.