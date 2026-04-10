#!/usr/bin/env python3
"""
Generates Java test classes from a PICT-generated TSV file.

Two modes:
  1. Per-TC classes: One FeatureMatrixTC{N}Test per row (for single-TC runs)
  2. Shard classes: FeatureMatrixShard{N}Test grouping ~5 TCs each (for CI/CD)

TCs sharing the same cluster config (S+C dimensions) are kept in the same shard
to avoid redundant cluster setup/teardown.

Usage:
    python3 generate_test_classes.py \
        --tsv path/to/generated-test-cases.tsv \
        --output-dir path/to/build/generated-test-sources/java/.../generated \
        [--shard-size 5]
"""

import argparse
import os
import sys
from collections import OrderedDict

# Maps TSV column names to DimensionId enum names
COLUMN_TO_DIMENSION_ID = {
    "W1_Topology": "W1_TOPOLOGY",
    "W2_NativeReplication": "W2_NATIVE_REPLICATION",
    "W3_ActiveActive": "W3_ACTIVE_ACTIVE",
    "W4_WriteCompute": "W4_WRITE_COMPUTE",
    "W5_Chunking": "W5_CHUNKING",
    "W6_RmdChunking": "W6_RMD_CHUNKING",
    "W7_IncrementalPush": "W7_INCREMENTAL_PUSH",
    "W8_Compression": "W8_COMPRESSION",
    "W9_DeferredSwap": "W9_DEFERRED_SWAP",
    "W10_TargetRegionPush": "W10_TARGET_REGION_PUSH",
    "W11_PushEngine": "W11_PUSH_ENGINE",
    "W12_TTLRepush": "W12_TTL_REPUSH",
    "W13_SeparateRTTopic": "W13_SEPARATE_RT_TOPIC",
    "R1_ClientType": "R1_CLIENT_TYPE",
    "R2_ReadCompute": "R2_READ_COMPUTE",
    "R3_DaVinciStorage": "R3_DAVINCI_STORAGE",
    "R4_FastRouting": "R4_FAST_ROUTING",
    "R5_LongTailRetry": "R5_LONG_TAIL_RETRY",
    "R6_RecordTransformer": "R6_RECORD_TRANSFORMER",
    "S1_ParallelBatchGet": "S1_PARALLEL_BATCH_GET",
    "S2_FastAvro": "S2_FAST_AVRO",
    "S3_AAWCParallel": "S3_AAWC_PARALLEL",
    "S4_BlobTransfer": "S4_BLOB_TRANSFER",
    "S5_QuotaEnforcement": "S5_QUOTA_ENFORCEMENT",
    "S6_AdaptiveThrottler": "S6_ADAPTIVE_THROTTLER",
    "C1_AADefaultHybrid": "C1_AA_DEFAULT_HYBRID",
    "C2_WCAutoHybridAA": "C2_WC_AUTO_HYBRID_AA",
    "C3_IncPushAutoHybridAA": "C3_INC_PUSH_AUTO_HYBRID_AA",
    "C4_SeparateRTAutoIncPush": "C4_SEPARATE_RT_AUTO_INC_PUSH",
    "C5_DeferredSwapService": "C5_DEFERRED_SWAP_SERVICE",
    "C6_SchemaValidation": "C6_SCHEMA_VALIDATION",
    "C7_BackupVersionCleanup": "C7_BACKUP_VERSION_CLEANUP",
    "C8_SystemStoreAutoMat": "C8_SYSTEM_STORE_AUTO_MAT",
    "C9_SupersetSchemaGen": "C9_SUPERSET_SCHEMA_GEN",
}

# Columns that form the cluster config key (Server + Controller dimensions)
CLUSTER_CONFIG_PREFIXES = ('S', 'C')

TC_TEMPLATE = '''\
package com.linkedin.venice.featurematrix.generated;

import com.linkedin.venice.featurematrix.AbstractFeatureMatrixTest;
import com.linkedin.venice.featurematrix.model.FeatureDimensions.DimensionId;
import com.linkedin.venice.featurematrix.model.TestCaseConfig;
import java.util.LinkedHashMap;
import java.util.Map;


/**
 * Generated test class for TC{tc_id}.
 * DO NOT EDIT — regenerate via: ./gradlew :tests:venice-feature-matrix-test:generateTestClasses
 */
public class FeatureMatrixTC{tc_id}Test extends AbstractFeatureMatrixTest {{
  @Override
  public TestCaseConfig getConfig() {{
    Map<DimensionId, String> dims = new LinkedHashMap<>();
{dim_puts}
    return new TestCaseConfig({tc_id}, dims);
  }}
}}
'''

SHARD_TEMPLATE = '''\
package com.linkedin.venice.featurematrix.generated;

import com.linkedin.venice.featurematrix.AbstractFeatureMatrixShardTest;
import com.linkedin.venice.featurematrix.model.FeatureDimensions.DimensionId;
import com.linkedin.venice.featurematrix.model.TestCaseConfig;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


/**
 * Generated shard test class (shard {shard_id}): TCs {tc_ids_str}.
 * DO NOT EDIT — regenerate via: ./gradlew :tests:venice-feature-matrix-test:generateTestClasses
 */
public class FeatureMatrixShard{shard_id}Test extends AbstractFeatureMatrixShardTest {{
  @Override
  public List<TestCaseConfig> getConfigs() {{
    List<TestCaseConfig> configs = new ArrayList<>();
{tc_blocks}
    return configs;
  }}
}}
'''

TC_BLOCK_TEMPLATE = '''\
    {{
      Map<DimensionId, String> dims = new LinkedHashMap<>();
{dim_puts}
      configs.add(new TestCaseConfig({tc_id}, dims));
    }}'''


def parse_tsv(tsv_path):
    """Parse TSV file, return (headers, rows)."""
    with open(tsv_path, 'r') as f:
        lines = [line.rstrip('\n') for line in f if line.strip()]
    headers = lines[0].split('\t')
    rows = []
    for line in lines[1:]:
        rows.append(line.split('\t'))
    return headers, rows


def cluster_config_key(headers, values):
    """Compute cluster config key from S+C columns."""
    parts = []
    for col_name, value in zip(headers, values):
        if col_name.startswith(CLUSTER_CONFIG_PREFIXES):
            parts.append(f"{col_name}={value}")
    return '|'.join(parts)


def make_dim_puts(headers, values, indent=4):
    """Generate dims.put(...) lines for a TC."""
    prefix = ' ' * indent
    lines = []
    for col_name, value in zip(headers, values):
        dimension_id = COLUMN_TO_DIMENSION_ID.get(col_name)
        if dimension_id is None:
            continue
        lines.append(f'{prefix}  dims.put(DimensionId.{dimension_id}, "{value}");')
    return '\n'.join(lines)


def group_into_shards(headers, rows, shard_size):
    """
    Group TCs into shards, keeping TCs with the same cluster config together.
    Returns list of shards, where each shard is a list of (tc_id, values).
    """
    # Group by cluster config key
    cluster_groups = OrderedDict()
    for tc_index, values in enumerate(rows):
        tc_id = tc_index + 1
        key = cluster_config_key(headers, values)
        cluster_groups.setdefault(key, []).append((tc_id, values))

    # Pack cluster groups into shards using first-fit
    shards = []
    current_shard = []
    for key, tcs in cluster_groups.items():
        if len(current_shard) + len(tcs) > shard_size and current_shard:
            shards.append(current_shard)
            current_shard = []
        current_shard.extend(tcs)
        if len(current_shard) >= shard_size:
            shards.append(current_shard)
            current_shard = []
    if current_shard:
        shards.append(current_shard)

    return shards


def clean_output_dir(output_dir):
    """Remove stale generated files."""
    if os.path.exists(output_dir):
        for f in os.listdir(output_dir):
            if (f.startswith('FeatureMatrixTC') or f.startswith('FeatureMatrixShard')) and f.endswith('Test.java'):
                os.remove(os.path.join(output_dir, f))


def generate_tc_class(tc_id, headers, values, output_dir):
    """Generate a single per-TC test class."""
    dim_puts = make_dim_puts(headers, values, indent=4)
    java_source = TC_TEMPLATE.format(tc_id=tc_id, dim_puts=dim_puts)
    file_path = os.path.join(output_dir, f'FeatureMatrixTC{tc_id}Test.java')
    with open(file_path, 'w') as f:
        f.write(java_source)
    return file_path


def generate_shard_class(shard_id, shard_tcs, headers, output_dir):
    """Generate a shard test class containing multiple TCs."""
    tc_blocks = []
    tc_ids = []
    for tc_id, values in shard_tcs:
        tc_ids.append(str(tc_id))
        dim_puts = make_dim_puts(headers, values, indent=6)
        tc_blocks.append(TC_BLOCK_TEMPLATE.format(tc_id=tc_id, dim_puts=dim_puts))

    tc_ids_str = ', '.join(tc_ids)
    java_source = SHARD_TEMPLATE.format(
        shard_id=shard_id,
        tc_ids_str=tc_ids_str,
        tc_blocks='\n'.join(tc_blocks),
    )
    file_path = os.path.join(output_dir, f'FeatureMatrixShard{shard_id}Test.java')
    with open(file_path, 'w') as f:
        f.write(java_source)
    return file_path


def main():
    parser = argparse.ArgumentParser(description='Generate feature matrix test classes from TSV')
    parser.add_argument('--tsv', required=True, help='Path to generated-test-cases.tsv')
    parser.add_argument('--output-dir', required=True, help='Output directory for generated Java files')
    parser.add_argument('--shard-size', type=int, default=5, help='Target number of TCs per shard (default: 5)')
    args = parser.parse_args()

    headers, rows = parse_tsv(args.tsv)

    unmapped = [h for h in headers if h not in COLUMN_TO_DIMENSION_ID]
    if unmapped:
        print(f"WARNING: Unmapped TSV columns: {unmapped}", file=sys.stderr)

    clean_output_dir(args.output_dir)
    os.makedirs(args.output_dir, exist_ok=True)

    # Generate per-TC classes
    tc_count = 0
    for tc_index, values in enumerate(rows):
        tc_id = tc_index + 1
        if len(values) != len(headers):
            print(f"WARNING: TC{tc_id} has {len(values)} values but {len(headers)} headers, skipping",
                  file=sys.stderr)
            continue
        generate_tc_class(tc_id, headers, values, args.output_dir)
        tc_count += 1

    # Generate shard classes
    shards = group_into_shards(headers, rows, args.shard_size)
    for shard_index, shard_tcs in enumerate(shards):
        shard_id = shard_index + 1
        generate_shard_class(shard_id, shard_tcs, headers, args.output_dir)

    print(f"Generated {tc_count} TC classes and {len(shards)} shard classes in {args.output_dir}")
    for i, shard in enumerate(shards, 1):
        tc_ids = [str(tc_id) for tc_id, _ in shard]
        print(f"  Shard {i}: {len(shard)} TCs [{', '.join(tc_ids)}]")


if __name__ == '__main__':
    main()
