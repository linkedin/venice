package com.linkedin.venice.meta;

public enum QueryAction {
  // STORAGE is a GET request to storage/storename/key on the router or storage/resourcename/partition/key on the
  // storage node
  STORAGE,

  // Health check request from routers
  HEALTH,

  // read-compute request from routers
  COMPUTE,

  // DICTIONARY is a GET request to storage/storename/version on the storage node to fetch compression dictionary for
  // that version
  DICTIONARY,

  // Admin request from server admin tool
  ADMIN,

  // METADATA is a GET request to /metadata/storename on the storage node to fetch metadata for that node
  METADATA,

  // CURRENT_VERSION is a GET request to /current_version/storename on the storage node to fetch current version for
  // that store
  CURRENT_VERSION,

  // TOPIC_PARTITION_INGESTION_CONTEXT is a GET request to /version topic/topic/partition from server admin tool
  TOPIC_PARTITION_INGESTION_CONTEXT,

  // BLOB_DISCOVERY is a GET request to blob_discovery/storeName/version/partition to find a node(s) with blobs
  BLOB_DISCOVERY
}
