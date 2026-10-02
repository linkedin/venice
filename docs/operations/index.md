# Operations Guide

This guide covers operational tasks for Venice administrators and operators.

## Data Management

- [Repush](data-management/repush.md) - Re-ingest data from source of truth to repair inconsistencies or apply schema
  changes
- [TTL](data-management/ttl.md) - Configure time-to-live to automatically expire old records
- [System Stores](data-management/system-stores.md) - Internal stores used by Venice for metadata and coordination

### Store Migration Schema IDs

Migration preserves every source value-schema ID, including gaps. A source with IDs `274`, `288`, and `315` produces
destination IDs `1`, `274`, `288`, and `315`: ordinary creation installs the first source schema at ID `1`, then
migration imports all original IDs. The extra `1` contains the same schema as `274`; it does not replace it.

Deploy the exact-ID import code on all participating controllers; the existing admin protocol needs no change. Each
import checks its requested ID and schema content, but migration does not audit the complete mapping after configuration
changes. Keep source schemas stable, pause new pushes during initial cloning, and verify regional mappings, ONLINE
versions, and reads using the original writer IDs before completing migration.

For a renumbered destination, use the supported abort/cleanup workflow while discovery still points to a healthy source,
then retry. Do not rewrite existing schema IDs or delete a destination already serving traffic without a recovery plan.
Keep the source until destination checks pass.

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
