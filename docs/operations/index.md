# Operations Guide

This guide covers operational tasks for Venice administrators and operators.

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
