/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.linkedin.venice.systemstore.schemas;

@SuppressWarnings("all")
/** This type contains all the store configs and the corresponding versions */
public class StoreProperties extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"StoreProperties\",\"namespace\":\"com.linkedin.venice.systemstore.schemas\",\"fields\":[{\"name\":\"name\",\"type\":\"string\",\"doc\":\"Store name.\"},{\"name\":\"owner\",\"type\":\"string\",\"doc\":\"Owner of this store.\"},{\"name\":\"createdTime\",\"type\":\"long\",\"doc\":\"Timestamp when this store was created.\"},{\"name\":\"currentVersion\",\"type\":\"int\",\"doc\":\"The number of version which is used currently.\",\"default\":0},{\"name\":\"partitionCount\",\"type\":\"int\",\"doc\":\"Default partition count for all of versions in this store. Once first version become online, the number will be assigned.\",\"default\":0},{\"name\":\"enableWrites\",\"type\":\"boolean\",\"doc\":\"If a store is disabled from writing, new version can not be created for it.\",\"default\":true},{\"name\":\"enableReads\",\"type\":\"boolean\",\"doc\":\"If a store is disabled from being read, none of versions under this store could serve read requests.\",\"default\":true},{\"name\":\"storageQuotaInByte\",\"type\":\"long\",\"doc\":\"Maximum capacity a store version is able to have, and default is 20GB\",\"default\":21474836480},{\"name\":\"persistenceType\",\"type\":\"int\",\"doc\":\"Type of persistence storage engine, and default is 'ROCKS_DB'\",\"default\":2},{\"name\":\"routingStrategy\",\"type\":\"int\",\"doc\":\"How to route the key to partition, and default is 'CONSISTENT_HASH'\",\"default\":0},{\"name\":\"readStrategy\",\"type\":\"int\",\"doc\":\"How to read data from multiple replications, and default is 'ANY_OF_ONLINE'\",\"default\":0},{\"name\":\"offlinePushStrategy\",\"type\":\"int\",\"doc\":\"When doing off-line push, how to decide the data is ready to serve, and default is 'WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION'\",\"default\":1},{\"name\":\"largestUsedVersionNumber\",\"type\":\"int\",\"doc\":\"The largest version number ever used before for this store.\",\"default\":0},{\"name\":\"readQuotaInCU\",\"type\":\"long\",\"doc\":\"Quota for read request hit this store. Measurement is capacity unit.\",\"default\":0},{\"name\":\"hybridConfig\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"StoreHybridConfig\",\"fields\":[{\"name\":\"rewindTimeInSeconds\",\"type\":\"long\"},{\"name\":\"offsetLagThresholdToGoOnline\",\"type\":\"long\"},{\"name\":\"producerTimestampLagThresholdToGoOnlineInSeconds\",\"type\":\"long\"}]}],\"doc\":\"Properties related to Hybrid Store behavior. If absent (null), then the store is not hybrid.\",\"default\":null},{\"name\":\"accessControlled\",\"type\":\"boolean\",\"doc\":\"Store-level ACL switch. When disabled, Venice Router should accept every request.\",\"default\":true},{\"name\":\"compressionStrategy\",\"type\":\"int\",\"doc\":\"Strategy used to compress/decompress Record's value, and default is 'NO_OP'\",\"default\":0},{\"name\":\"clientDecompressionEnabled\",\"type\":\"boolean\",\"doc\":\"le/Disable client-side record decompression (default: true)\",\"default\":true},{\"name\":\"chunkingEnabled\",\"type\":\"boolean\",\"doc\":\"Whether current store supports large value (typically more than 1MB). By default, the chunking feature is disabled.\",\"default\":false},{\"name\":\"batchGetLimit\",\"type\":\"int\",\"doc\":\"Batch get key number limit, and Venice will use cluster-level config if it is not positive.\",\"default\":-1},{\"name\":\"numVersionsToPreserve\",\"type\":\"int\",\"doc\":\"How many versions this store preserve at most. By default it's 0 means we use the cluster level config to determine how many version is preserved.\",\"default\":0},{\"name\":\"incrementalPushEnabled\",\"type\":\"boolean\",\"doc\":\"Flag to see if the store supports incremental push or not\",\"default\":false},{\"name\":\"migrating\",\"type\":\"boolean\",\"doc\":\"Whether or not the store is in the process of migration.\",\"default\":false},{\"name\":\"writeComputationEnabled\",\"type\":\"boolean\",\"doc\":\"Whether or not write-path computation feature is enabled for this store.\",\"default\":false},{\"name\":\"readComputationEnabled\",\"type\":\"boolean\",\"doc\":\"Whether read-path computation is enabled for this store.\",\"default\":false},{\"name\":\"bootstrapToOnlineTimeoutInHours\",\"type\":\"int\",\"doc\":\"Maximum number of hours allowed for the store to transition from bootstrap to online state.\",\"default\":24},{\"name\":\"leaderFollowerModelEnabled\",\"type\":\"boolean\",\"doc\":\"Whether or not to use leader follower state transition model for upcoming version.\",\"default\":false},{\"name\":\"nativeReplicationEnabled\",\"type\":\"boolean\",\"doc\":\"Whether or not native should be enabled for this store.  Will only successfully apply if leaderFollowerModelEnabled is also true either in this update or a previous version of the store.\",\"default\":false},{\"name\":\"pushStreamSourceAddress\",\"type\":\"string\",\"doc\":\"Address to the kafka broker which holds the source of truth topic for this store version.\",\"default\":\"\"},{\"name\":\"backupStrategy\",\"type\":\"int\",\"doc\":\"Strategies to store backup versions, and default is 'DELETE_ON_NEW_PUSH_START'\",\"default\":1},{\"name\":\"schemaAutoRegisteFromPushJobEnabled\",\"type\":\"boolean\",\"doc\":\"Whether or not value schema auto registration enabled from push job for this store.\",\"default\":false},{\"name\":\"latestSuperSetValueSchemaId\",\"type\":\"int\",\"doc\":\"For read compute stores with auto super-set schema enabled, stores the latest super-set value schema ID.\",\"default\":-1},{\"name\":\"hybridStoreDiskQuotaEnabled\",\"type\":\"boolean\",\"doc\":\"Whether or not storage disk quota is enabled for a hybrid store. This store config cannot be enabled until the routers and servers in the corresponding cluster are upgraded to the right version: 0.2.249 or above for routers and servers.\",\"default\":false},{\"name\":\"storeMetadataSystemStoreEnabled\",\"type\":\"boolean\",\"doc\":\"Whether or not the store metadata system store is enabled for this store.\",\"default\":false},{\"name\":\"etlConfig\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"StoreETLConfig\",\"fields\":[{\"name\":\"etledUserProxyAccount\",\"type\":\"string\",\"doc\":\"If enabled regular ETL or future version ETL, this account name is part of path for where the ETLed snapshots will go. for example, for user account veniceetl001, snapshots will be published to HDFS /jobs/veniceetl001/storeName.\"},{\"name\":\"regularVersionETLEnabled\",\"type\":\"boolean\",\"doc\":\"Whether or not enable regular version ETL for this store.\"},{\"name\":\"futureVersionETLEnabled\",\"type\":\"boolean\",\"doc\":\"Whether or not enable future version ETL - the version that might come online in future - for this store.\"}]}],\"doc\":\"Properties related to ETL Store behavior.\",\"default\":null},{\"name\":\"partitionerConfig\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"StorePartitionerConfig\",\"fields\":[{\"name\":\"partitionerClass\",\"type\":\"string\"},{\"name\":\"partitionerParams\",\"type\":{\"type\":\"map\",\"values\":\"string\"}},{\"name\":\"amplificationFactor\",\"type\":\"int\"}]}],\"doc\":\"\",\"default\":null},{\"name\":\"incrementalPushPolicy\",\"type\":\"int\",\"doc\":\"Incremental Push Policy to reconcile with real time pushes, and default is 'PUSH_TO_VERSION_TOPIC'\",\"default\":0},{\"name\":\"latestVersionPromoteToCurrentTimestamp\",\"type\":\"long\",\"doc\":\"This is used to track the time when a new version is promoted to current version. For now, it is mostly to decide whether a backup version can be removed or not based on retention. For the existing store before this code change, it will be set to be current timestamp.\",\"default\":-1},{\"name\":\"backupVersionRetentionMs\",\"type\":\"long\",\"doc\":\"Backup retention time, and if it is not set (-1), Venice Controller will use the default configured retention. {@link com.linkedin.venice.ConfigKeys#CONTROLLER_BACKUP_VERSION_DEFAULT_RETENTION_MS}.\",\"default\":-1},{\"name\":\"replicationFactor\",\"type\":\"int\",\"doc\":\"The number of replica each store version will keep.\",\"default\":3},{\"name\":\"migrationDuplicateStore\",\"type\":\"boolean\",\"doc\":\"Whether or not the store is a duplicate store in the process of migration.\",\"default\":false},{\"name\":\"nativeReplicationSourceFabric\",\"type\":\"string\",\"doc\":\"The source fabric name to be uses in native replication. Remote consumption will happen from kafka in this fabric.\",\"default\":\"\"},{\"name\":\"daVinciPushStatusStoreEnabled\",\"type\":\"boolean\",\"doc\":\"Whether or not davinci push status store is enabled.\",\"default\":false},{\"name\":\"storeMetaSystemStoreEnabled\",\"type\":\"boolean\",\"doc\":\"Whether or not the store meta system store is enabled for this store.\",\"default\":false},{\"name\":\"activeActiveReplicationEnabled\",\"type\":\"boolean\",\"doc\":\"Whether or not active/active replication is enabled for hybrid stores; eventually this config will replace native replication flag, when all stores are on A/A\",\"default\":false},{\"name\":\"versions\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"StoreVersion\",\"fields\":[{\"name\":\"storeName\",\"type\":\"string\",\"doc\":\"Name of the store which this version belong to.\"},{\"name\":\"number\",\"type\":\"int\",\"doc\":\"Version number.\"},{\"name\":\"createdTime\",\"type\":\"long\",\"doc\":\"Time when this version was created.\"},{\"name\":\"status\",\"type\":\"int\",\"doc\":\"Status of version, and default is 'STARTED'\",\"default\":1},{\"name\":\"pushJobId\",\"type\":\"string\",\"default\":\"\"},{\"name\":\"compressionStrategy\",\"type\":\"int\",\"doc\":\"strategies used to compress/decompress Record's value, and default is 'NO_OP'\",\"default\":0},{\"name\":\"leaderFollowerModelEnabled\",\"type\":\"boolean\",\"doc\":\"Whether or not to use leader follower state transition.\",\"default\":false},{\"name\":\"nativeReplicationEnabled\",\"type\":\"boolean\",\"doc\":\"Whether or not native replication is enabled.\",\"default\":false},{\"name\":\"pushStreamSourceAddress\",\"type\":\"string\",\"doc\":\"Address to the kafka broker which holds the source of truth topic for this store version.\",\"default\":\"\"},{\"name\":\"bufferReplayEnabledForHybrid\",\"type\":\"boolean\",\"doc\":\"Whether or not to enable buffer replay for hybrid.\",\"default\":true},{\"name\":\"chunkingEnabled\",\"type\":\"boolean\",\"doc\":\"Whether or not large values are supported (via chunking).\",\"default\":false},{\"name\":\"pushType\",\"type\":\"int\",\"doc\":\"Producer type for this version, and default is 'BATCH'\",\"default\":0},{\"name\":\"partitionCount\",\"type\":\"int\",\"doc\":\"Partition count of this version.\",\"default\":0},{\"name\":\"partitionerConfig\",\"type\":[\"null\",\"StorePartitionerConfig\"],\"doc\":\"Config for custom partitioning.\",\"default\":null},{\"name\":\"incrementalPushPolicy\",\"type\":\"int\",\"doc\":\"Incremental Push Policy to reconcile with real time pushes., and default is 'PUSH_TO_VERSION_TOPIC'\",\"default\":0},{\"name\":\"replicationFactor\",\"type\":\"int\",\"doc\":\"The number of replica this store version is keeping.\",\"default\":3},{\"name\":\"nativeReplicationSourceFabric\",\"type\":\"string\",\"doc\":\"The source fabric name to be uses in native replication. Remote consumption will happen from kafka in this fabric.\",\"default\":\"\"},{\"name\":\"incrementalPushEnabled\",\"type\":\"boolean\",\"doc\":\"Flag to see if the store supports incremental push or not\",\"default\":false},{\"name\":\"useVersionLevelIncrementalPushEnabled\",\"type\":\"boolean\",\"doc\":\"Flag to see if incrementalPushEnabled config at StoreVersion should be used. This is needed during migration of this config from Store level to Version level. We can deprecate this field later.\",\"default\":false},{\"name\":\"hybridConfig\",\"type\":[\"null\",\"StoreHybridConfig\"],\"doc\":\"Properties related to Hybrid Store behavior. If absent (null), then the store is not hybrid.\",\"default\":null},{\"name\":\"useVersionLevelHybridConfig\",\"type\":\"boolean\",\"doc\":\"Flag to see if hybridConfig at StoreVersion should be used. This is needed during migration of this config from Store level to Version level. We can deprecate this field later.\",\"default\":false},{\"name\":\"activeActiveReplicationEnabled\",\"type\":\"boolean\",\"doc\":\"Whether or not active/active replication is enabled for hybrid stores; eventually this config will replace native replication flag, when all stores are on A/A\",\"default\":false}]}},\"doc\":\"List of non-retired versions. It's currently sorted and there is code run under the assumption that the last element in the list is the largest. Check out {VeniceHelixAdmin#getIncrementalPushVersion}, and please make it in mind if you want to change this logic\",\"default\":[]},{\"name\":\"systemStores\",\"type\":{\"type\":\"map\",\"values\":{\"type\":\"record\",\"name\":\"SystemStoreProperties\",\"fields\":[{\"name\":\"largestUsedVersionNumber\",\"type\":\"int\",\"default\":0},{\"name\":\"currentVersion\",\"type\":\"int\",\"default\":0},{\"name\":\"latestVersionPromoteToCurrentTimestamp\",\"type\":\"long\",\"default\":-1},{\"name\":\"versions\",\"type\":{\"type\":\"array\",\"items\":\"StoreVersion\"},\"default\":[]}]}},\"doc\":\"This field is used to maintain a mapping between each type of system store and the corresponding distinct properties\",\"default\":{}}]}");
  /** Store name. */
  public java.lang.CharSequence name;
  /** Owner of this store. */
  public java.lang.CharSequence owner;
  /** Timestamp when this store was created. */
  public long createdTime;
  /** The number of version which is used currently. */
  public int currentVersion;
  /** Default partition count for all of versions in this store. Once first version become online, the number will be assigned. */
  public int partitionCount;
  /** If a store is disabled from writing, new version can not be created for it. */
  public boolean enableWrites;
  /** If a store is disabled from being read, none of versions under this store could serve read requests. */
  public boolean enableReads;
  /** Maximum capacity a store version is able to have, and default is 20GB */
  public long storageQuotaInByte;
  /** Type of persistence storage engine, and default is 'ROCKS_DB' */
  public int persistenceType;
  /** How to route the key to partition, and default is 'CONSISTENT_HASH' */
  public int routingStrategy;
  /** How to read data from multiple replications, and default is 'ANY_OF_ONLINE' */
  public int readStrategy;
  /** When doing off-line push, how to decide the data is ready to serve, and default is 'WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION' */
  public int offlinePushStrategy;
  /** The largest version number ever used before for this store. */
  public int largestUsedVersionNumber;
  /** Quota for read request hit this store. Measurement is capacity unit. */
  public long readQuotaInCU;
  /** Properties related to Hybrid Store behavior. If absent (null), then the store is not hybrid. */
  public com.linkedin.venice.systemstore.schemas.StoreHybridConfig hybridConfig;
  /** Store-level ACL switch. When disabled, Venice Router should accept every request. */
  public boolean accessControlled;
  /** Strategy used to compress/decompress Record's value, and default is 'NO_OP' */
  public int compressionStrategy;
  /** le/Disable client-side record decompression (default: true) */
  public boolean clientDecompressionEnabled;
  /** Whether current store supports large value (typically more than 1MB). By default, the chunking feature is disabled. */
  public boolean chunkingEnabled;
  /** Batch get key number limit, and Venice will use cluster-level config if it is not positive. */
  public int batchGetLimit;
  /** How many versions this store preserve at most. By default it's 0 means we use the cluster level config to determine how many version is preserved. */
  public int numVersionsToPreserve;
  /** Flag to see if the store supports incremental push or not */
  public boolean incrementalPushEnabled;
  /** Whether or not the store is in the process of migration. */
  public boolean migrating;
  /** Whether or not write-path computation feature is enabled for this store. */
  public boolean writeComputationEnabled;
  /** Whether read-path computation is enabled for this store. */
  public boolean readComputationEnabled;
  /** Maximum number of hours allowed for the store to transition from bootstrap to online state. */
  public int bootstrapToOnlineTimeoutInHours;
  /** Whether or not to use leader follower state transition model for upcoming version. */
  public boolean leaderFollowerModelEnabled;
  /** Whether or not native should be enabled for this store.  Will only successfully apply if leaderFollowerModelEnabled is also true either in this update or a previous version of the store. */
  public boolean nativeReplicationEnabled;
  /** Address to the kafka broker which holds the source of truth topic for this store version. */
  public java.lang.CharSequence pushStreamSourceAddress;
  /** Strategies to store backup versions, and default is 'DELETE_ON_NEW_PUSH_START' */
  public int backupStrategy;
  /** Whether or not value schema auto registration enabled from push job for this store. */
  public boolean schemaAutoRegisteFromPushJobEnabled;
  /** For read compute stores with auto super-set schema enabled, stores the latest super-set value schema ID. */
  public int latestSuperSetValueSchemaId;
  /** Whether or not storage disk quota is enabled for a hybrid store. This store config cannot be enabled until the routers and servers in the corresponding cluster are upgraded to the right version: 0.2.249 or above for routers and servers. */
  public boolean hybridStoreDiskQuotaEnabled;
  /** Whether or not the store metadata system store is enabled for this store. */
  public boolean storeMetadataSystemStoreEnabled;
  /** Properties related to ETL Store behavior. */
  public com.linkedin.venice.systemstore.schemas.StoreETLConfig etlConfig;
  /**  */
  public com.linkedin.venice.systemstore.schemas.StorePartitionerConfig partitionerConfig;
  /** Incremental Push Policy to reconcile with real time pushes, and default is 'PUSH_TO_VERSION_TOPIC' */
  public int incrementalPushPolicy;
  /** This is used to track the time when a new version is promoted to current version. For now, it is mostly to decide whether a backup version can be removed or not based on retention. For the existing store before this code change, it will be set to be current timestamp. */
  public long latestVersionPromoteToCurrentTimestamp;
  /** Backup retention time, and if it is not set (-1), Venice Controller will use the default configured retention. {@link com.linkedin.venice.ConfigKeys#CONTROLLER_BACKUP_VERSION_DEFAULT_RETENTION_MS}. */
  public long backupVersionRetentionMs;
  /** The number of replica each store version will keep. */
  public int replicationFactor;
  /** Whether or not the store is a duplicate store in the process of migration. */
  public boolean migrationDuplicateStore;
  /** The source fabric name to be uses in native replication. Remote consumption will happen from kafka in this fabric. */
  public java.lang.CharSequence nativeReplicationSourceFabric;
  /** Whether or not davinci push status store is enabled. */
  public boolean daVinciPushStatusStoreEnabled;
  /** Whether or not the store meta system store is enabled for this store. */
  public boolean storeMetaSystemStoreEnabled;
  /** Whether or not active/active replication is enabled for hybrid stores; eventually this config will replace native replication flag, when all stores are on A/A */
  public boolean activeActiveReplicationEnabled;
  /** List of non-retired versions. It's currently sorted and there is code run under the assumption that the last element in the list is the largest. Check out {VeniceHelixAdmin#getIncrementalPushVersion}, and please make it in mind if you want to change this logic */
  public java.util.List<com.linkedin.venice.systemstore.schemas.StoreVersion> versions;
  /** This field is used to maintain a mapping between each type of system store and the corresponding distinct properties */
  public java.util.Map<java.lang.CharSequence,com.linkedin.venice.systemstore.schemas.SystemStoreProperties> systemStores;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return name;
    case 1: return owner;
    case 2: return createdTime;
    case 3: return currentVersion;
    case 4: return partitionCount;
    case 5: return enableWrites;
    case 6: return enableReads;
    case 7: return storageQuotaInByte;
    case 8: return persistenceType;
    case 9: return routingStrategy;
    case 10: return readStrategy;
    case 11: return offlinePushStrategy;
    case 12: return largestUsedVersionNumber;
    case 13: return readQuotaInCU;
    case 14: return hybridConfig;
    case 15: return accessControlled;
    case 16: return compressionStrategy;
    case 17: return clientDecompressionEnabled;
    case 18: return chunkingEnabled;
    case 19: return batchGetLimit;
    case 20: return numVersionsToPreserve;
    case 21: return incrementalPushEnabled;
    case 22: return migrating;
    case 23: return writeComputationEnabled;
    case 24: return readComputationEnabled;
    case 25: return bootstrapToOnlineTimeoutInHours;
    case 26: return leaderFollowerModelEnabled;
    case 27: return nativeReplicationEnabled;
    case 28: return pushStreamSourceAddress;
    case 29: return backupStrategy;
    case 30: return schemaAutoRegisteFromPushJobEnabled;
    case 31: return latestSuperSetValueSchemaId;
    case 32: return hybridStoreDiskQuotaEnabled;
    case 33: return storeMetadataSystemStoreEnabled;
    case 34: return etlConfig;
    case 35: return partitionerConfig;
    case 36: return incrementalPushPolicy;
    case 37: return latestVersionPromoteToCurrentTimestamp;
    case 38: return backupVersionRetentionMs;
    case 39: return replicationFactor;
    case 40: return migrationDuplicateStore;
    case 41: return nativeReplicationSourceFabric;
    case 42: return daVinciPushStatusStoreEnabled;
    case 43: return storeMetaSystemStoreEnabled;
    case 44: return activeActiveReplicationEnabled;
    case 45: return versions;
    case 46: return systemStores;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: name = (java.lang.CharSequence)value$; break;
    case 1: owner = (java.lang.CharSequence)value$; break;
    case 2: createdTime = (java.lang.Long)value$; break;
    case 3: currentVersion = (java.lang.Integer)value$; break;
    case 4: partitionCount = (java.lang.Integer)value$; break;
    case 5: enableWrites = (java.lang.Boolean)value$; break;
    case 6: enableReads = (java.lang.Boolean)value$; break;
    case 7: storageQuotaInByte = (java.lang.Long)value$; break;
    case 8: persistenceType = (java.lang.Integer)value$; break;
    case 9: routingStrategy = (java.lang.Integer)value$; break;
    case 10: readStrategy = (java.lang.Integer)value$; break;
    case 11: offlinePushStrategy = (java.lang.Integer)value$; break;
    case 12: largestUsedVersionNumber = (java.lang.Integer)value$; break;
    case 13: readQuotaInCU = (java.lang.Long)value$; break;
    case 14: hybridConfig = (com.linkedin.venice.systemstore.schemas.StoreHybridConfig)value$; break;
    case 15: accessControlled = (java.lang.Boolean)value$; break;
    case 16: compressionStrategy = (java.lang.Integer)value$; break;
    case 17: clientDecompressionEnabled = (java.lang.Boolean)value$; break;
    case 18: chunkingEnabled = (java.lang.Boolean)value$; break;
    case 19: batchGetLimit = (java.lang.Integer)value$; break;
    case 20: numVersionsToPreserve = (java.lang.Integer)value$; break;
    case 21: incrementalPushEnabled = (java.lang.Boolean)value$; break;
    case 22: migrating = (java.lang.Boolean)value$; break;
    case 23: writeComputationEnabled = (java.lang.Boolean)value$; break;
    case 24: readComputationEnabled = (java.lang.Boolean)value$; break;
    case 25: bootstrapToOnlineTimeoutInHours = (java.lang.Integer)value$; break;
    case 26: leaderFollowerModelEnabled = (java.lang.Boolean)value$; break;
    case 27: nativeReplicationEnabled = (java.lang.Boolean)value$; break;
    case 28: pushStreamSourceAddress = (java.lang.CharSequence)value$; break;
    case 29: backupStrategy = (java.lang.Integer)value$; break;
    case 30: schemaAutoRegisteFromPushJobEnabled = (java.lang.Boolean)value$; break;
    case 31: latestSuperSetValueSchemaId = (java.lang.Integer)value$; break;
    case 32: hybridStoreDiskQuotaEnabled = (java.lang.Boolean)value$; break;
    case 33: storeMetadataSystemStoreEnabled = (java.lang.Boolean)value$; break;
    case 34: etlConfig = (com.linkedin.venice.systemstore.schemas.StoreETLConfig)value$; break;
    case 35: partitionerConfig = (com.linkedin.venice.systemstore.schemas.StorePartitionerConfig)value$; break;
    case 36: incrementalPushPolicy = (java.lang.Integer)value$; break;
    case 37: latestVersionPromoteToCurrentTimestamp = (java.lang.Long)value$; break;
    case 38: backupVersionRetentionMs = (java.lang.Long)value$; break;
    case 39: replicationFactor = (java.lang.Integer)value$; break;
    case 40: migrationDuplicateStore = (java.lang.Boolean)value$; break;
    case 41: nativeReplicationSourceFabric = (java.lang.CharSequence)value$; break;
    case 42: daVinciPushStatusStoreEnabled = (java.lang.Boolean)value$; break;
    case 43: storeMetaSystemStoreEnabled = (java.lang.Boolean)value$; break;
    case 44: activeActiveReplicationEnabled = (java.lang.Boolean)value$; break;
    case 45: versions = (java.util.List<com.linkedin.venice.systemstore.schemas.StoreVersion>)value$; break;
    case 46: systemStores = (java.util.Map<java.lang.CharSequence,com.linkedin.venice.systemstore.schemas.SystemStoreProperties>)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
