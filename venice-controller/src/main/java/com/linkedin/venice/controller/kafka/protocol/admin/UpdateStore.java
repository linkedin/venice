/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.linkedin.venice.controller.kafka.protocol.admin;

@SuppressWarnings("all")
public class UpdateStore extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"UpdateStore\",\"namespace\":\"com.linkedin.venice.controller.kafka.protocol.admin\",\"fields\":[{\"name\":\"clusterName\",\"type\":\"string\"},{\"name\":\"storeName\",\"type\":\"string\"},{\"name\":\"owner\",\"type\":\"string\"},{\"name\":\"partitionNum\",\"type\":\"int\"},{\"name\":\"currentVersion\",\"type\":\"int\"},{\"name\":\"enableReads\",\"type\":\"boolean\"},{\"name\":\"enableWrites\",\"type\":\"boolean\"},{\"name\":\"storageQuotaInByte\",\"type\":\"long\",\"default\":21474836480},{\"name\":\"readQuotaInCU\",\"type\":\"long\",\"default\":1800},{\"name\":\"hybridStoreConfig\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"HybridStoreConfigRecord\",\"fields\":[{\"name\":\"rewindTimeInSeconds\",\"type\":\"long\"},{\"name\":\"offsetLagThresholdToGoOnline\",\"type\":\"long\"},{\"name\":\"producerTimestampLagThresholdToGoOnlineInSeconds\",\"type\":\"long\",\"default\":-1},{\"name\":\"dataReplicationPolicy\",\"type\":\"int\",\"doc\":\"Real-time Samza job data replication policy. Using int because Avro Enums are not evolvable 0 => NON_AGGREGATE, 1 => AGGREGATE, 2 => ACTIVE_ACTIVE\",\"default\":0}]}],\"default\":null},{\"name\":\"accessControlled\",\"type\":\"boolean\",\"default\":false},{\"name\":\"compressionStrategy\",\"type\":\"int\",\"doc\":\"Using int because Avro Enums are not evolvable\",\"default\":0},{\"name\":\"chunkingEnabled\",\"type\":\"boolean\",\"default\":false},{\"name\":\"singleGetRouterCacheEnabled\",\"type\":\"boolean\",\"default\":false},{\"name\":\"batchGetRouterCacheEnabled\",\"type\":\"boolean\",\"default\":false},{\"name\":\"batchGetLimit\",\"type\":\"int\",\"doc\":\"The max key number allowed in batch get request, and Venice will use cluster-level config if the limit (not positive) is not valid\",\"default\":-1},{\"name\":\"numVersionsToPreserve\",\"type\":\"int\",\"doc\":\"The max number of versions the store should preserve. Venice will use cluster-level config if the number is 0 here.\",\"default\":0},{\"name\":\"incrementalPushEnabled\",\"type\":\"boolean\",\"doc\":\"a flag to see if the store supports incremental push or not\",\"default\":false},{\"name\":\"isMigrating\",\"type\":\"boolean\",\"doc\":\"Whether or not the store is in the process of migration\",\"default\":false},{\"name\":\"writeComputationEnabled\",\"type\":\"boolean\",\"doc\":\"Whether write-path computation feature is enabled for this store\",\"default\":false},{\"name\":\"readComputationEnabled\",\"type\":\"boolean\",\"doc\":\"Whether read-path computation feature is enabled for this store\",\"default\":false},{\"name\":\"bootstrapToOnlineTimeoutInHours\",\"type\":\"int\",\"doc\":\"Maximum number of hours allowed for the store to transition from bootstrap to online state\",\"default\":24},{\"name\":\"leaderFollowerModelEnabled\",\"type\":\"boolean\",\"doc\":\"Whether or not to use leader follower state transition model for upcoming version\",\"default\":false},{\"name\":\"backupStrategy\",\"type\":\"int\",\"doc\":\"Strategies to store backup versions.\",\"default\":0},{\"name\":\"clientDecompressionEnabled\",\"type\":\"boolean\",\"default\":true},{\"name\":\"schemaAutoRegisterFromPushJobEnabled\",\"type\":\"boolean\",\"default\":false},{\"name\":\"hybridStoreOverheadBypass\",\"type\":\"boolean\",\"default\":false},{\"name\":\"hybridStoreDiskQuotaEnabled\",\"type\":\"boolean\",\"doc\":\"Whether or not to enable disk storage quota for a hybrid store\",\"default\":false},{\"name\":\"ETLStoreConfig\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"ETLStoreConfigRecord\",\"fields\":[{\"name\":\"etledUserProxyAccount\",\"type\":[\"null\",\"string\"]},{\"name\":\"regularVersionETLEnabled\",\"type\":\"boolean\"},{\"name\":\"futureVersionETLEnabled\",\"type\":\"boolean\"}]}],\"default\":null},{\"name\":\"partitionerConfig\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"PartitionerConfigRecord\",\"fields\":[{\"name\":\"partitionerClass\",\"type\":\"string\"},{\"name\":\"partitionerParams\",\"type\":{\"type\":\"map\",\"values\":\"string\"}},{\"name\":\"amplificationFactor\",\"type\":\"int\"}]}],\"default\":null},{\"name\":\"nativeReplicationEnabled\",\"type\":\"boolean\",\"default\":false},{\"name\":\"pushStreamSourceAddress\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"largestUsedVersionNumber\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"incrementalPushPolicy\",\"type\":\"int\",\"doc\":\"Incremental Push Policy to reconcile with real time pushes. Using int because Avro Enums are not evolvable 0 => PUSH_TO_VERSION_TOPIC, 1 => INCREMENTAL_PUSH_SAME_AS_REAL_TIME\",\"default\":0},{\"name\":\"backupVersionRetentionMs\",\"type\":\"long\",\"doc\":\"Backup version retention time after a new version is promoted to the current version, if not specified, Venice will use the configured retention as the default policy\",\"default\":-1},{\"name\":\"replicationFactor\",\"type\":\"int\",\"doc\":\"number of replica each store version will have\",\"default\":3},{\"name\":\"migrationDuplicateStore\",\"type\":\"boolean\",\"doc\":\"Whether or not the store is a duplicate store in the process of migration\",\"default\":false},{\"name\":\"nativeReplicationSourceFabric\",\"type\":[\"null\",\"string\"],\"doc\":\"The source fabric to be used when the store is running in Native Replication mode.\",\"default\":null},{\"name\":\"activeActiveReplicationEnabled\",\"type\":\"boolean\",\"doc\":\"A command option to enable/disable Active/Active replication feature for a store\",\"default\":false},{\"name\":\"updatedConfigsList\",\"type\":{\"type\":\"array\",\"items\":\"string\"},\"doc\":\"The list that contains all updated configs by the UpdateStore command. Most of the fields in UpdateStore are not optional, and changing those fields to Optional (Union) is not a backward compatible change, so we have to add an addition array field to record all updated configs in parent controller.\",\"default\":[]},{\"name\":\"replicateAllConfigs\",\"type\":\"boolean\",\"doc\":\"A flag to indicate whether all store configs in parent cluster will be replicated to child clusters; true by default, so that existing UpdateStore messages in Admin topic will behave the same as before.\",\"default\":true},{\"name\":\"regionsFilter\",\"type\":[\"null\",\"string\"],\"doc\":\"A list of regions that will be impacted by the UpdateStore command\",\"default\":null}]}");
  public java.lang.CharSequence clusterName;
  public java.lang.CharSequence storeName;
  public java.lang.CharSequence owner;
  public int partitionNum;
  public int currentVersion;
  public boolean enableReads;
  public boolean enableWrites;
  public long storageQuotaInByte;
  public long readQuotaInCU;
  public com.linkedin.venice.controller.kafka.protocol.admin.HybridStoreConfigRecord hybridStoreConfig;
  public boolean accessControlled;
  /** Using int because Avro Enums are not evolvable */
  public int compressionStrategy;
  public boolean chunkingEnabled;
  public boolean singleGetRouterCacheEnabled;
  public boolean batchGetRouterCacheEnabled;
  /** The max key number allowed in batch get request, and Venice will use cluster-level config if the limit (not positive) is not valid */
  public int batchGetLimit;
  /** The max number of versions the store should preserve. Venice will use cluster-level config if the number is 0 here. */
  public int numVersionsToPreserve;
  /** a flag to see if the store supports incremental push or not */
  public boolean incrementalPushEnabled;
  /** Whether or not the store is in the process of migration */
  public boolean isMigrating;
  /** Whether write-path computation feature is enabled for this store */
  public boolean writeComputationEnabled;
  /** Whether read-path computation feature is enabled for this store */
  public boolean readComputationEnabled;
  /** Maximum number of hours allowed for the store to transition from bootstrap to online state */
  public int bootstrapToOnlineTimeoutInHours;
  /** Whether or not to use leader follower state transition model for upcoming version */
  public boolean leaderFollowerModelEnabled;
  /** Strategies to store backup versions. */
  public int backupStrategy;
  public boolean clientDecompressionEnabled;
  public boolean schemaAutoRegisterFromPushJobEnabled;
  public boolean hybridStoreOverheadBypass;
  /** Whether or not to enable disk storage quota for a hybrid store */
  public boolean hybridStoreDiskQuotaEnabled;
  public com.linkedin.venice.controller.kafka.protocol.admin.ETLStoreConfigRecord ETLStoreConfig;
  public com.linkedin.venice.controller.kafka.protocol.admin.PartitionerConfigRecord partitionerConfig;
  public boolean nativeReplicationEnabled;
  public java.lang.CharSequence pushStreamSourceAddress;
  public java.lang.Integer largestUsedVersionNumber;
  /** Incremental Push Policy to reconcile with real time pushes. Using int because Avro Enums are not evolvable 0 => PUSH_TO_VERSION_TOPIC, 1 => INCREMENTAL_PUSH_SAME_AS_REAL_TIME */
  public int incrementalPushPolicy;
  /** Backup version retention time after a new version is promoted to the current version, if not specified, Venice will use the configured retention as the default policy */
  public long backupVersionRetentionMs;
  /** number of replica each store version will have */
  public int replicationFactor;
  /** Whether or not the store is a duplicate store in the process of migration */
  public boolean migrationDuplicateStore;
  /** The source fabric to be used when the store is running in Native Replication mode. */
  public java.lang.CharSequence nativeReplicationSourceFabric;
  /** A command option to enable/disable Active/Active replication feature for a store */
  public boolean activeActiveReplicationEnabled;
  /** The list that contains all updated configs by the UpdateStore command. Most of the fields in UpdateStore are not optional, and changing those fields to Optional (Union) is not a backward compatible change, so we have to add an addition array field to record all updated configs in parent controller. */
  public java.util.List<java.lang.CharSequence> updatedConfigsList;
  /** A flag to indicate whether all store configs in parent cluster will be replicated to child clusters; true by default, so that existing UpdateStore messages in Admin topic will behave the same as before. */
  public boolean replicateAllConfigs;
  /** A list of regions that will be impacted by the UpdateStore command */
  public java.lang.CharSequence regionsFilter;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return clusterName;
    case 1: return storeName;
    case 2: return owner;
    case 3: return partitionNum;
    case 4: return currentVersion;
    case 5: return enableReads;
    case 6: return enableWrites;
    case 7: return storageQuotaInByte;
    case 8: return readQuotaInCU;
    case 9: return hybridStoreConfig;
    case 10: return accessControlled;
    case 11: return compressionStrategy;
    case 12: return chunkingEnabled;
    case 13: return singleGetRouterCacheEnabled;
    case 14: return batchGetRouterCacheEnabled;
    case 15: return batchGetLimit;
    case 16: return numVersionsToPreserve;
    case 17: return incrementalPushEnabled;
    case 18: return isMigrating;
    case 19: return writeComputationEnabled;
    case 20: return readComputationEnabled;
    case 21: return bootstrapToOnlineTimeoutInHours;
    case 22: return leaderFollowerModelEnabled;
    case 23: return backupStrategy;
    case 24: return clientDecompressionEnabled;
    case 25: return schemaAutoRegisterFromPushJobEnabled;
    case 26: return hybridStoreOverheadBypass;
    case 27: return hybridStoreDiskQuotaEnabled;
    case 28: return ETLStoreConfig;
    case 29: return partitionerConfig;
    case 30: return nativeReplicationEnabled;
    case 31: return pushStreamSourceAddress;
    case 32: return largestUsedVersionNumber;
    case 33: return incrementalPushPolicy;
    case 34: return backupVersionRetentionMs;
    case 35: return replicationFactor;
    case 36: return migrationDuplicateStore;
    case 37: return nativeReplicationSourceFabric;
    case 38: return activeActiveReplicationEnabled;
    case 39: return updatedConfigsList;
    case 40: return replicateAllConfigs;
    case 41: return regionsFilter;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: clusterName = (java.lang.CharSequence)value$; break;
    case 1: storeName = (java.lang.CharSequence)value$; break;
    case 2: owner = (java.lang.CharSequence)value$; break;
    case 3: partitionNum = (java.lang.Integer)value$; break;
    case 4: currentVersion = (java.lang.Integer)value$; break;
    case 5: enableReads = (java.lang.Boolean)value$; break;
    case 6: enableWrites = (java.lang.Boolean)value$; break;
    case 7: storageQuotaInByte = (java.lang.Long)value$; break;
    case 8: readQuotaInCU = (java.lang.Long)value$; break;
    case 9: hybridStoreConfig = (com.linkedin.venice.controller.kafka.protocol.admin.HybridStoreConfigRecord)value$; break;
    case 10: accessControlled = (java.lang.Boolean)value$; break;
    case 11: compressionStrategy = (java.lang.Integer)value$; break;
    case 12: chunkingEnabled = (java.lang.Boolean)value$; break;
    case 13: singleGetRouterCacheEnabled = (java.lang.Boolean)value$; break;
    case 14: batchGetRouterCacheEnabled = (java.lang.Boolean)value$; break;
    case 15: batchGetLimit = (java.lang.Integer)value$; break;
    case 16: numVersionsToPreserve = (java.lang.Integer)value$; break;
    case 17: incrementalPushEnabled = (java.lang.Boolean)value$; break;
    case 18: isMigrating = (java.lang.Boolean)value$; break;
    case 19: writeComputationEnabled = (java.lang.Boolean)value$; break;
    case 20: readComputationEnabled = (java.lang.Boolean)value$; break;
    case 21: bootstrapToOnlineTimeoutInHours = (java.lang.Integer)value$; break;
    case 22: leaderFollowerModelEnabled = (java.lang.Boolean)value$; break;
    case 23: backupStrategy = (java.lang.Integer)value$; break;
    case 24: clientDecompressionEnabled = (java.lang.Boolean)value$; break;
    case 25: schemaAutoRegisterFromPushJobEnabled = (java.lang.Boolean)value$; break;
    case 26: hybridStoreOverheadBypass = (java.lang.Boolean)value$; break;
    case 27: hybridStoreDiskQuotaEnabled = (java.lang.Boolean)value$; break;
    case 28: ETLStoreConfig = (com.linkedin.venice.controller.kafka.protocol.admin.ETLStoreConfigRecord)value$; break;
    case 29: partitionerConfig = (com.linkedin.venice.controller.kafka.protocol.admin.PartitionerConfigRecord)value$; break;
    case 30: nativeReplicationEnabled = (java.lang.Boolean)value$; break;
    case 31: pushStreamSourceAddress = (java.lang.CharSequence)value$; break;
    case 32: largestUsedVersionNumber = (java.lang.Integer)value$; break;
    case 33: incrementalPushPolicy = (java.lang.Integer)value$; break;
    case 34: backupVersionRetentionMs = (java.lang.Long)value$; break;
    case 35: replicationFactor = (java.lang.Integer)value$; break;
    case 36: migrationDuplicateStore = (java.lang.Boolean)value$; break;
    case 37: nativeReplicationSourceFabric = (java.lang.CharSequence)value$; break;
    case 38: activeActiveReplicationEnabled = (java.lang.Boolean)value$; break;
    case 39: updatedConfigsList = (java.util.List<java.lang.CharSequence>)value$; break;
    case 40: replicateAllConfigs = (java.lang.Boolean)value$; break;
    case 41: regionsFilter = (java.lang.CharSequence)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
