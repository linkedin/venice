#!/bin/bash

# Venice Documentation Migration Script
# This script reorganizes documentation from Jekyll structure to MkDocs structure

set -e

DOCS="docs"
BACKUP="docs-backup-$(date +%Y%m%d-%H%M%S)"

echo "Creating backup..."
cp -r "$DOCS" "$BACKUP"

echo "Creating directories..."
mkdir -p "$DOCS"/{getting-started,user-guide/{concepts,write-apis,read-apis,design-patterns},operations/{data-management,advanced},contributing/{development,architecture,documentation,proposals},resources}

echo "Moving files (no modifications)..."

# Getting Started
mv "$DOCS/quickstart/quickstart-single-datacenter.md" "$DOCS/getting-started/quickstart-single-dc.md"
mv "$DOCS/quickstart/quickstart-multi-datacenter.md" "$DOCS/getting-started/quickstart-multi-dc.md"

# User Guide - Concepts
mv "$DOCS/user_guide/ttl.md" "$DOCS/user-guide/concepts/ttl.md"

# User Guide - Write APIs
mv "$DOCS/user_guide/write_api/push_job.md" "$DOCS/user-guide/write-apis/batch-push.md"
mv "$DOCS/user_guide/write_api/stream_processor.md" "$DOCS/user-guide/write-apis/stream-processor.md"
mv "$DOCS/user_guide/write_api/online_producer.md" "$DOCS/user-guide/write-apis/online-producer.md"

# User Guide - Read APIs
mv "$DOCS/user_guide/read_api/da_vinci_client.md" "$DOCS/user-guide/read-apis/da-vinci-client.md"

# User Guide - Design Patterns
mv "$DOCS/user_guide/design_patterns/merging_batch_and_rt_data.md" "$DOCS/user-guide/design-patterns/merging-batch-and-rt.md"

# Operations
mv "$DOCS/ops_guide/repush.md" "$DOCS/operations/data-management/repush.md"
mv "$DOCS/ops_guide/system_stores.md" "$DOCS/operations/data-management/system-stores.md"
mv "$DOCS/quickstart/quickstart_P2P_transfer_bootstrapping.md" "$DOCS/operations/advanced/p2p-bootstrapping.md"
mv "$DOCS/quickstart/quickstart_data_integrity_validation.md" "$DOCS/operations/advanced/data-integrity.md"

# Contributing - Development
mv "$DOCS/dev_guide/how_to/workspace_setup.md" "$DOCS/contributing/development/workspace-setup.md"
mv "$DOCS/dev_guide/how_to/recommended_development_workflow.md" "$DOCS/contributing/development/dev-workflow.md"
mv "$DOCS/dev_guide/how_to/code_coverage_guide.md" "$DOCS/contributing/development/testing.md"
mv "$DOCS/dev_guide/how_to/style_guide.md" "$DOCS/contributing/development/style-guide.md"

# Contributing - Architecture
mv "$DOCS/dev_guide/navigating_project.md" "$DOCS/contributing/architecture/navigation.md"
mv "$DOCS/dev_guide/venice_write_path.md" "$DOCS/contributing/architecture/write-path.md"
mv "$DOCS/dev_guide/java.md" "$DOCS/contributing/architecture/java-internals.md"
mv "$DOCS/dev_guide/router_rest_spec.md" "$DOCS/contributing/architecture/router-api.md"

# Contributing - Documentation
mv "$DOCS/dev_guide/how_to/doc_guide.md" "$DOCS/contributing/documentation/writing-docs.md"
mv "$DOCS/dev_guide/how_to/design_doc.md" "$DOCS/contributing/documentation/design-docs.md"

# Contributing - Root
mv "$DOCS/dev_guide/how_to/CODE_OF_CONDUCT.md" "$DOCS/contributing/code-of-conduct.md"
mv "$DOCS/dev_guide/how_to/CONTRIBUTING.md" "$DOCS/contributing/contributing.md"
mv "$DOCS/dev_guide/how_to/SECURITY.md" "$DOCS/contributing/security.md"

# Contributing - Proposals
mv "$DOCS/proposals/"* "$DOCS/contributing/proposals/"

# Resources
mv "$DOCS/user_guide/learn_more.md" "$DOCS/resources/learn-more.md"

# Cleanup
rm -rf "$DOCS"/{quickstart,user_guide,ops_guide,dev_guide,proposals}
rm -f "$DOCS"/{_config.yml,Gemfile}
rm -rf "$DOCS/_sass"

echo "Migration complete. Backup: $BACKUP"
