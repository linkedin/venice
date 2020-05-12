/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.linkedin.venice.meta.systemstore.schemas;

@SuppressWarnings("all")
public class StoreMetadataValue extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = org.apache.avro.Schema.parse("{\"type\":\"record\",\"name\":\"StoreMetadataValue\",\"namespace\":\"com.linkedin.venice.meta.systemstore.schemas\",\"fields\":[{\"name\":\"timestamp\",\"type\":\"long\",\"doc\":\"Timestamp when the value or a partial update for the value was generated by the writer (Venice controller).\"},{\"name\":\"metadataUnion\",\"type\":[{\"type\":\"record\",\"name\":\"StoreAttributes\",\"fields\":[{\"name\":\"configs\",\"type\":{\"type\":\"record\",\"name\":\"StoreProperties\",\"fields\":[{\"name\":\"accessControlled\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"backupStrategy\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"batchGetLimit\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"batchGetRouterCacheEnabled\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"bootstrapToOnlineTimeoutInHours\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"chunkingEnabled\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"clientDecompressionEnabled\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"compressionStrategy\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"createdTime\",\"type\":\"long\"},{\"name\":\"currentVersion\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"enableReads\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"enableWrites\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"etlStoreConfig\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"ETLStoreConfig\",\"fields\":[{\"name\":\"etledUserProxyAccount\",\"type\":\"string\"},{\"name\":\"futureVersionETLEnabled\",\"type\":\"boolean\"},{\"name\":\"regularVersionETLEnabled\",\"type\":\"boolean\"}]}],\"default\":null},{\"name\":\"hybrid\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"hybridStoreConfig\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"HybridStoreConfig\",\"fields\":[{\"name\":\"offsetLagThresholdToGoOnline\",\"type\":\"long\"},{\"name\":\"rewindTimeInSeconds\",\"type\":\"long\"}]}],\"default\":null},{\"name\":\"hybridStoreDiskQuotaEnabled\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"incrementalPushEnabled\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"largestUsedVersionNumber\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"latestSuperSetValueSchemaId\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"leaderFollowerModelEnabled\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"migrating\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"numVersionsToPreserve\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"offLinePushStrategy\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"owner\",\"type\":\"string\"},{\"name\":\"partitionCount\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"partitionerConfig\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"PartitionerConfig\",\"fields\":[{\"name\":\"amplificationFactor\",\"type\":\"int\"},{\"name\":\"partitionerClass\",\"type\":\"string\"},{\"name\":\"partitionerParams\",\"type\":{\"type\":\"map\",\"values\":\"string\"}}]}],\"default\":null},{\"name\":\"readComputationEnabled\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"readQuotaInCU\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"readStrategy\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"routingStrategy\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"schemaAutoRegisterFromPushJobEnabled\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"singleGetRouterCacheEnabled\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"storageQuotaInByte\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"superSetSchemaAutoGenerationForReadComputeEnabled\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"systemStore\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"nativeReplicationEnabled\",\"type\":\"boolean\",\"default\":false},{\"name\":\"pushStreamSourceAddress\",\"type\":[\"null\",\"string\"],\"default\":null}]}},{\"name\":\"sourceCluster\",\"type\":\"string\",\"doc\":\"The source Venice cluster of the store. Usually it's the first Venice cluster that the store was created/materialized in.\"},{\"name\":\"otherClusters\",\"type\":{\"type\":\"array\",\"items\":\"string\"},\"doc\":\"Other Venice clusters that the store is materializing in either because of store migration or offset load from the source cluster.\"}]},{\"type\":\"record\",\"name\":\"TargetVersionStates\",\"fields\":[{\"name\":\"targetVersionStates\",\"type\":{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"StoreVersionState\",\"fields\":[{\"name\":\"versionNumber\",\"type\":\"int\"},{\"name\":\"pushJobId\",\"type\":\"string\"},{\"name\":\"partitionCount\",\"type\":\"int\"},{\"name\":\"creationTime\",\"type\":\"long\"},{\"name\":\"chunkingEnabled\",\"type\":\"boolean\"},{\"name\":\"compressionStrategy\",\"type\":\"string\"},{\"name\":\"leaderFollowerModelEnabled\",\"type\":\"boolean\"},{\"name\":\"pushType\",\"type\":\"string\"},{\"name\":\"status\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"partitionerConfig\",\"type\":[\"null\",\"PartitionerConfig\"],\"default\":null}]}},\"doc\":\"An array of version states that should be bootstrapped in each fabric. Old entries are purged from the array as new entries are added to keep the array size bounded.\"}]},{\"type\":\"record\",\"name\":\"CurrentStoreStates\",\"fields\":[{\"name\":\"states\",\"type\":\"StoreProperties\"}]},{\"type\":\"record\",\"name\":\"CurrentVersionStates\",\"fields\":[{\"name\":\"currentVersionStates\",\"type\":{\"type\":\"array\",\"items\":\"StoreVersionState\"},\"doc\":\"An array of version states describing the current state of an given fabric. Old entries are purged as new entries are added.\"},{\"name\":\"currentVersion\",\"type\":\"int\",\"doc\":\"The version number that is currently used to serve read traffic\"}]}],\"doc\":\"Contains different types of store metadata\"}]}");
  /** Timestamp when the value or a partial update for the value was generated by the writer (Venice controller). */
  public long timestamp;
  /** Contains different types of store metadata */
  public java.lang.Object metadataUnion;
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return timestamp;
    case 1: return metadataUnion;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: timestamp = (java.lang.Long)value$; break;
    case 1: metadataUnion = (java.lang.Object)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
}
