/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.linkedin.venice.meta.systemstore.schemas;

@SuppressWarnings("all")
public class StoreProperties extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"StoreProperties\",\"namespace\":\"com.linkedin.venice.meta.systemstore.schemas\",\"fields\":[{\"name\":\"accessControlled\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"backupStrategy\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"batchGetLimit\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"batchGetRouterCacheEnabled\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"bootstrapToOnlineTimeoutInHours\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"chunkingEnabled\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"clientDecompressionEnabled\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"compressionStrategy\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"createdTime\",\"type\":\"long\"},{\"name\":\"currentVersion\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"enableReads\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"enableWrites\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"etlStoreConfig\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"ETLStoreConfig\",\"fields\":[{\"name\":\"etledUserProxyAccount\",\"type\":\"string\"},{\"name\":\"futureVersionETLEnabled\",\"type\":\"boolean\"},{\"name\":\"regularVersionETLEnabled\",\"type\":\"boolean\"}]}],\"default\":null},{\"name\":\"hybrid\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"hybridStoreConfig\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"HybridStoreConfig\",\"fields\":[{\"name\":\"offsetLagThresholdToGoOnline\",\"type\":\"long\"},{\"name\":\"rewindTimeInSeconds\",\"type\":\"long\"},{\"name\":\"producerTimestampLagThresholdToGoOnlineInSeconds\",\"type\":\"long\",\"default\":-1}]}],\"default\":null},{\"name\":\"hybridStoreDiskQuotaEnabled\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"incrementalPushEnabled\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"largestUsedVersionNumber\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"latestSuperSetValueSchemaId\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"leaderFollowerModelEnabled\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"migrating\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"numVersionsToPreserve\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"offLinePushStrategy\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"owner\",\"type\":\"string\"},{\"name\":\"partitionCount\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"partitionerConfig\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"PartitionerConfig\",\"fields\":[{\"name\":\"amplificationFactor\",\"type\":\"int\"},{\"name\":\"partitionerClass\",\"type\":\"string\"},{\"name\":\"partitionerParams\",\"type\":{\"type\":\"map\",\"values\":\"string\"}}]}],\"default\":null},{\"name\":\"persistenceType\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"readComputationEnabled\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"readQuotaInCU\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"readStrategy\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"routingStrategy\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"schemaAutoRegisterFromPushJobEnabled\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"singleGetRouterCacheEnabled\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"storageQuotaInByte\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"superSetSchemaAutoGenerationForReadComputeEnabled\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"systemStore\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"nativeReplicationEnabled\",\"type\":\"boolean\",\"default\":false},{\"name\":\"pushStreamSourceAddress\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"writeComputationEnabled\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"daVinciPushStatusStoreEnabled\",\"type\":[\"null\",\"boolean\"],\"default\":null}]}");
  public java.lang.Boolean accessControlled;
  public java.lang.CharSequence backupStrategy;
  public java.lang.Integer batchGetLimit;
  public java.lang.Boolean batchGetRouterCacheEnabled;
  public java.lang.Integer bootstrapToOnlineTimeoutInHours;
  public java.lang.Boolean chunkingEnabled;
  public java.lang.Boolean clientDecompressionEnabled;
  public java.lang.CharSequence compressionStrategy;
  public long createdTime;
  public java.lang.Integer currentVersion;
  public java.lang.Boolean enableReads;
  public java.lang.Boolean enableWrites;
  public com.linkedin.venice.meta.systemstore.schemas.ETLStoreConfig etlStoreConfig;
  public java.lang.Boolean hybrid;
  public com.linkedin.venice.meta.systemstore.schemas.HybridStoreConfig hybridStoreConfig;
  public java.lang.Boolean hybridStoreDiskQuotaEnabled;
  public java.lang.Boolean incrementalPushEnabled;
  public java.lang.Integer largestUsedVersionNumber;
  public java.lang.Integer latestSuperSetValueSchemaId;
  public java.lang.Boolean leaderFollowerModelEnabled;
  public java.lang.Boolean migrating;
  public java.lang.CharSequence name;
  public java.lang.Integer numVersionsToPreserve;
  public java.lang.CharSequence offLinePushStrategy;
  public java.lang.CharSequence owner;
  public java.lang.Integer partitionCount;
  public com.linkedin.venice.meta.systemstore.schemas.PartitionerConfig partitionerConfig;
  public java.lang.CharSequence persistenceType;
  public java.lang.Boolean readComputationEnabled;
  public java.lang.Long readQuotaInCU;
  public java.lang.CharSequence readStrategy;
  public java.lang.CharSequence routingStrategy;
  public java.lang.Boolean schemaAutoRegisterFromPushJobEnabled;
  public java.lang.Boolean singleGetRouterCacheEnabled;
  public java.lang.Long storageQuotaInByte;
  public java.lang.Boolean superSetSchemaAutoGenerationForReadComputeEnabled;
  public java.lang.Boolean systemStore;
  public boolean nativeReplicationEnabled;
  public java.lang.CharSequence pushStreamSourceAddress;
  public java.lang.Boolean writeComputationEnabled;
  public java.lang.Boolean daVinciPushStatusStoreEnabled;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return accessControlled;
    case 1: return backupStrategy;
    case 2: return batchGetLimit;
    case 3: return batchGetRouterCacheEnabled;
    case 4: return bootstrapToOnlineTimeoutInHours;
    case 5: return chunkingEnabled;
    case 6: return clientDecompressionEnabled;
    case 7: return compressionStrategy;
    case 8: return createdTime;
    case 9: return currentVersion;
    case 10: return enableReads;
    case 11: return enableWrites;
    case 12: return etlStoreConfig;
    case 13: return hybrid;
    case 14: return hybridStoreConfig;
    case 15: return hybridStoreDiskQuotaEnabled;
    case 16: return incrementalPushEnabled;
    case 17: return largestUsedVersionNumber;
    case 18: return latestSuperSetValueSchemaId;
    case 19: return leaderFollowerModelEnabled;
    case 20: return migrating;
    case 21: return name;
    case 22: return numVersionsToPreserve;
    case 23: return offLinePushStrategy;
    case 24: return owner;
    case 25: return partitionCount;
    case 26: return partitionerConfig;
    case 27: return persistenceType;
    case 28: return readComputationEnabled;
    case 29: return readQuotaInCU;
    case 30: return readStrategy;
    case 31: return routingStrategy;
    case 32: return schemaAutoRegisterFromPushJobEnabled;
    case 33: return singleGetRouterCacheEnabled;
    case 34: return storageQuotaInByte;
    case 35: return superSetSchemaAutoGenerationForReadComputeEnabled;
    case 36: return systemStore;
    case 37: return nativeReplicationEnabled;
    case 38: return pushStreamSourceAddress;
    case 39: return writeComputationEnabled;
    case 40: return daVinciPushStatusStoreEnabled;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: accessControlled = (java.lang.Boolean)value$; break;
    case 1: backupStrategy = (java.lang.CharSequence)value$; break;
    case 2: batchGetLimit = (java.lang.Integer)value$; break;
    case 3: batchGetRouterCacheEnabled = (java.lang.Boolean)value$; break;
    case 4: bootstrapToOnlineTimeoutInHours = (java.lang.Integer)value$; break;
    case 5: chunkingEnabled = (java.lang.Boolean)value$; break;
    case 6: clientDecompressionEnabled = (java.lang.Boolean)value$; break;
    case 7: compressionStrategy = (java.lang.CharSequence)value$; break;
    case 8: createdTime = (java.lang.Long)value$; break;
    case 9: currentVersion = (java.lang.Integer)value$; break;
    case 10: enableReads = (java.lang.Boolean)value$; break;
    case 11: enableWrites = (java.lang.Boolean)value$; break;
    case 12: etlStoreConfig = (com.linkedin.venice.meta.systemstore.schemas.ETLStoreConfig)value$; break;
    case 13: hybrid = (java.lang.Boolean)value$; break;
    case 14: hybridStoreConfig = (com.linkedin.venice.meta.systemstore.schemas.HybridStoreConfig)value$; break;
    case 15: hybridStoreDiskQuotaEnabled = (java.lang.Boolean)value$; break;
    case 16: incrementalPushEnabled = (java.lang.Boolean)value$; break;
    case 17: largestUsedVersionNumber = (java.lang.Integer)value$; break;
    case 18: latestSuperSetValueSchemaId = (java.lang.Integer)value$; break;
    case 19: leaderFollowerModelEnabled = (java.lang.Boolean)value$; break;
    case 20: migrating = (java.lang.Boolean)value$; break;
    case 21: name = (java.lang.CharSequence)value$; break;
    case 22: numVersionsToPreserve = (java.lang.Integer)value$; break;
    case 23: offLinePushStrategy = (java.lang.CharSequence)value$; break;
    case 24: owner = (java.lang.CharSequence)value$; break;
    case 25: partitionCount = (java.lang.Integer)value$; break;
    case 26: partitionerConfig = (com.linkedin.venice.meta.systemstore.schemas.PartitionerConfig)value$; break;
    case 27: persistenceType = (java.lang.CharSequence)value$; break;
    case 28: readComputationEnabled = (java.lang.Boolean)value$; break;
    case 29: readQuotaInCU = (java.lang.Long)value$; break;
    case 30: readStrategy = (java.lang.CharSequence)value$; break;
    case 31: routingStrategy = (java.lang.CharSequence)value$; break;
    case 32: schemaAutoRegisterFromPushJobEnabled = (java.lang.Boolean)value$; break;
    case 33: singleGetRouterCacheEnabled = (java.lang.Boolean)value$; break;
    case 34: storageQuotaInByte = (java.lang.Long)value$; break;
    case 35: superSetSchemaAutoGenerationForReadComputeEnabled = (java.lang.Boolean)value$; break;
    case 36: systemStore = (java.lang.Boolean)value$; break;
    case 37: nativeReplicationEnabled = (java.lang.Boolean)value$; break;
    case 38: pushStreamSourceAddress = (java.lang.CharSequence)value$; break;
    case 39: writeComputationEnabled = (java.lang.Boolean)value$; break;
    case 40: daVinciPushStatusStoreEnabled = (java.lang.Boolean)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
