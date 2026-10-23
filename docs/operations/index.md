# Operations Guide

This guide covers operational tasks for Venice administrators and operators.

## Cloud Configuration Compatibility

`controller.helix.cloud.id` and its `CONTROLLER_HELIX_CLOUD_ID` constant have been retired. The controller ignores
existing values and no longer emits Helix's `CLOUD_ID` field. Cloud enablement, providers, information sources and
processor selection are unchanged; existing stored CloudConfig records are not rewritten.

`HelixUtils.getCloudConfig` no longer takes a cloud-ID argument. Update and rebuild its callers. Release this writer
migration before adopting a Helix version that removes the cloud-ID APIs; this change does not upgrade the Helix
dependency. If a custom processor reads the ID, migrate that reader before writers stop emitting it. There is no
replacement identifier setting.

## Data Management

- [Repush](data-management/repush.md) - Re-ingest data from source of truth to repair inconsistencies or apply schema
  changes
- [TTL](data-management/ttl.md) - Configure time-to-live to automatically expire old records
- [System Stores](data-management/system-stores.md) - Internal stores used by Venice for metadata and coordination

## Alerting

- [Oncall Runbook](alerting/oncall-runbook.md) - Investigation and remediation steps for common Venice alerts

## Advanced Topics

- [P2P Bootstrapping](advanced/p2p-bootstrapping.md) - Peer-to-peer data transfer for faster server and client
  bootstrapping
- [Data Integrity](advanced/data-integrity.md) - Verify data consistency and detect corruption
- [Ingestion Pipeline Debugging](advanced/ingestion-pipeline-debugging.md) - Stage-by-stage guide for debugging
  ingestion slowdowns and heartbeat delay alerts
- [Collecting Diagnostics](advanced/collecting-diagnostics.md) - How to capture heap dumps, thread dumps, and JFR
  profiles for debugging Venice issues
