package com.linkedin.venice.meta;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.exceptions.VeniceException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;


/**
 * This interface defines all the public APIs, and if you need to add accessors to
 * some new fields, this interface needs to be changed accordingly.
 *
 * IMPORTANT: getter functions must start with `is` for boolean result and `get` for other types, and setter functions
 *            must start with `set`!
 */
public interface Store {
  /**
   * Special version number indicates none of version is available to read.
   */
  int NON_EXISTING_VERSION = 0;

  /**
   * Default value of numVersionPreserve, by default we should use cluster level config instead of store level config.
   */
  int NUM_VERSION_PRESERVE_NOT_SET = 0;

  String SYSTEM_STORE_NAME_PREFIX = "venice_system_store_";
  String SYSTEM_STORE_FORMAT = SYSTEM_STORE_NAME_PREFIX + "%s";

  long UNLIMITED_STORAGE_QUOTA = -1;

  int IGNORE_VERSION = -1;

  int BOOTSTRAP_TO_ONLINE_TIMEOUT_IN_HOURS = 24;

  long DEFAULT_RT_RETENTION_TIME = TimeUnit.DAYS.toMillis(5);

  /**
   * Store name rules:
   *  1.  Only letters, numbers, underscore or dash
   *  2. No double dashes
   */

  Pattern storeNamePattern = Pattern.compile("^[a-zA-Z0-9_-]+$");

  static boolean isValidStoreName(String name) {
    Matcher matcher = storeNamePattern.matcher(name);
    return matcher.matches() && !name.contains("--");
  }

  /**
   * This function is used to pre-fill the default value defined in schema.
   * So far, it only support the followings:
   * 1. Only top-level fields are supported.
   * 2. All the primitive types are supported.
   * 3. For union, only `null` default is supported.
   * 4. For array/map, only empty list/map are supported.
   *
   * Other than the above, this function will throw exception.
   *
   * TODO: once the whole stack migrates to the modern avro version (avro-1.7+), we could leverage
   * SpecificRecord builder to prefill the default value, which will be much more powerful.
   * @param recordType
   * @param <T>
   * @return
   */
  static <T extends SpecificRecord> T prefillAvroRecordWithDefaultValue(T recordType) {
    Schema schema = recordType.getSchema();
    for (Schema.Field field: schema.getFields()) {
      if (AvroCompatibilityHelper.fieldHasDefault(field)) {
        // has default
        Object defaultValue = AvroCompatibilityHelper.getSpecificDefaultValue(field);
        Schema.Type fieldType = field.schema().getType();
        switch (fieldType) {
          case NULL:
            throw new VeniceException("Default value for `null` type is not expected");
          case BOOLEAN:
          case INT:
          case LONG:
          case FLOAT:
          case DOUBLE:
          case STRING:
            recordType.put(field.pos(), defaultValue);
            break;
          case UNION:
            if (defaultValue == null) {
              recordType.put(field.pos(), null);
            } else {
              throw new VeniceException("Non 'null' default value is not supported for union type: " + field.name());
            }
            break;
          case ARRAY:
            Collection collection = (Collection) defaultValue;
            if (collection.isEmpty()) {
              recordType.put(field.pos(), new ArrayList<>());
            } else {
              throw new VeniceException(
                  "Non 'empty array' default value is not supported for array type: " + field.name());
            }
            break;
          case MAP:
            Map map = (Map) defaultValue;
            if (map.isEmpty()) {
              recordType.put(field.pos(), new HashMap<>());
            } else {
              throw new VeniceException("Non 'empty map' default value is not supported for map type: " + field.name());
            }
            break;
          case ENUM:
          case FIXED:
          case BYTES:
          case RECORD:
            throw new VeniceException(
                "Default value for field: " + field.name() + " with type: " + fieldType + " is not supported");
        }
      }
    }

    return recordType;
  }

  static boolean isSystemStore(String storeName) {
    return storeName.startsWith(SYSTEM_STORE_NAME_PREFIX);
  }

  String getName();

  String getOwner();

  void setOwner(String owner);

  long getCreatedTime();

  int getCurrentVersion();

  void setCurrentVersion(int currentVersion);

  void setCurrentVersionWithoutCheck(int currentVersion);

  long getLowWatermark();

  void setLowWatermark(long lowWatermark);

  PersistenceType getPersistenceType();

  void setPersistenceType(PersistenceType persistenceType);

  RoutingStrategy getRoutingStrategy();

  ReadStrategy getReadStrategy();

  OfflinePushStrategy getOffLinePushStrategy();

  int getLargestUsedVersionNumber();

  void setLargestUsedVersionNumber(int largestUsedVersionNumber);

  long getStorageQuotaInByte();

  void setStorageQuotaInByte(long storageQuotaInByte);

  int getPartitionCount();

  void setPartitionCount(int partitionCount);

  PartitionerConfig getPartitionerConfig();

  void setPartitionerConfig(PartitionerConfig value);

  boolean isEnableWrites();

  void setEnableWrites(boolean enableWrites);

  boolean isEnableReads();

  void setEnableReads(boolean enableReads);

  long getReadQuotaInCU();

  void setReadQuotaInCU(long readQuotaInCU);

  HybridStoreConfig getHybridStoreConfig();

  void setHybridStoreConfig(HybridStoreConfig hybridStoreConfig);

  Map<String, ViewConfig> getViewConfigs();

  void setViewConfigs(Map<String, ViewConfig> viewConfigMap);

  boolean isHybrid();

  CompressionStrategy getCompressionStrategy();

  void setCompressionStrategy(CompressionStrategy compressionStrategy);

  boolean getClientDecompressionEnabled();

  void setClientDecompressionEnabled(boolean clientDecompressionEnabled);

  boolean isChunkingEnabled();

  void setChunkingEnabled(boolean chunkingEnabled);

  boolean isRmdChunkingEnabled();

  void setRmdChunkingEnabled(boolean rmdChunkingEnabled);

  int getBatchGetLimit();

  void setBatchGetLimit(int batchGetLimit);

  boolean isIncrementalPushEnabled();

  void setIncrementalPushEnabled(boolean incrementalPushEnabled);

  boolean isAccessControlled();

  void setAccessControlled(boolean accessControlled);

  boolean isMigrating();

  void setMigrating(boolean migrating);

  int getNumVersionsToPreserve();

  void setNumVersionsToPreserve(int numVersionsToPreserve);

  boolean isWriteComputationEnabled();

  void setWriteComputationEnabled(boolean writeComputationEnabled);

  boolean isReadComputationEnabled();

  void setReadComputationEnabled(boolean readComputationEnabled);

  int getBootstrapToOnlineTimeoutInHours();

  void setBootstrapToOnlineTimeoutInHours(int bootstrapToOnlineTimeoutInHours);

  String getPushStreamSourceAddress();

  void setPushStreamSourceAddress(String sourceAddress);

  boolean isNativeReplicationEnabled();

  Optional<Integer> getRmdVersionID();

  void setRmdVersionID(Optional<Integer> rmdVersionID);

  void setNativeReplicationEnabled(boolean nativeReplicationEnabled);

  BackupStrategy getBackupStrategy();

  void setBackupStrategy(BackupStrategy value);

  boolean isSchemaAutoRegisterFromPushJobEnabled();

  void setSchemaAutoRegisterFromPushJobEnabled(boolean value);

  int getLatestSuperSetValueSchemaId();

  void setLatestSuperSetValueSchemaId(int valueSchemaId);

  boolean isHybridStoreDiskQuotaEnabled();

  void setHybridStoreDiskQuotaEnabled(boolean enabled);

  ETLStoreConfig getEtlStoreConfig();

  void setEtlStoreConfig(ETLStoreConfig etlStoreConfig);

  boolean isStoreMetadataSystemStoreEnabled();

  void setStoreMetadataSystemStoreEnabled(boolean storeMetadataSystemStoreEnabled);

  boolean isStoreMetaSystemStoreEnabled();

  void setStoreMetaSystemStoreEnabled(boolean storeMetaSystemStoreEnabled);

  long getLatestVersionPromoteToCurrentTimestamp();

  void setLatestVersionPromoteToCurrentTimestamp(long latestVersionPromoteToCurrentTimestamp);

  long getBackupVersionRetentionMs();

  void setBackupVersionRetentionMs(long backupVersionRetentionMs);

  long getRetentionTime();

  int getReplicationFactor();

  void setReplicationFactor(int replicationFactor);

  boolean isMigrationDuplicateStore();

  void setMigrationDuplicateStore(boolean migrationDuplicateStore);

  String getNativeReplicationSourceFabric();

  void setNativeReplicationSourceFabric(String nativeReplicationSourceFabric);

  boolean isActiveActiveReplicationEnabled();

  void setActiveActiveReplicationEnabled(boolean activeActiveReplicationEnabled);

  Map<String, SystemStoreAttributes> getSystemStores();

  void setSystemStores(Map<String, SystemStoreAttributes> systemStores);

  void putSystemStore(VeniceSystemStoreType systemStoreType, SystemStoreAttributes systemStoreAttributes);

  boolean isDaVinciPushStatusStoreEnabled();

  void setDaVinciPushStatusStoreEnabled(boolean daVinciPushStatusStoreEnabled);

  Store cloneStore();

  List<Version> getVersions();

  void setVersions(List<Version> versions);

  Optional<CompressionStrategy> getVersionCompressionStrategy(int versionNumber);

  void setBufferReplayForHybridForVersion(int versionNum, boolean enabled);

  void addVersion(Version version);

  void forceAddVersion(Version version, boolean isClonedVersion);

  void checkDisableStoreWrite(String action, int version);

  Version deleteVersion(int versionNumber);

  boolean containsVersion(int versionNumber);

  void updateVersionStatus(int versionNumber, VersionStatus status);

  Version peekNextVersion();

  Optional<Version> getVersion(int versionNumber);

  VersionStatus getVersionStatus(int versionNumber);

  List<Version> retrieveVersionsToDelete(int clusterNumVersionsToPreserve);

  boolean isSystemStore();

  void fixMissingFields();
}
