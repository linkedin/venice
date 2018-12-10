package com.linkedin.venice.meta;

import com.linkedin.venice.compression.CompressionStrategy;
import java.util.List;
import java.util.Map;

import static com.linkedin.venice.meta.Store.*;


/**
 * Json-serializable class for sending store information to the controller client
 */
@com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = true)
@org.codehaus.jackson.annotate.JsonIgnoreProperties(ignoreUnknown = true)
public class StoreInfo {
  public static StoreInfo fromStore(Store store){
    StoreInfo storeInfo = new StoreInfo();
    storeInfo.setName(store.getName());
    storeInfo.setOwner(store.getOwner());
    storeInfo.setCurrentVersion(store.getCurrentVersion());
    storeInfo.setPartitionCount(store.getPartitionCount());
    storeInfo.setEnableStoreWrites(store.isEnableWrites());
    storeInfo.setEnableStoreReads(store.isEnableReads());
    storeInfo.setStorageQuotaInByte(store.getStorageQuotaInByte());
    storeInfo.setReadQuotaInCU(store.getReadQuotaInCU());
    storeInfo.setVersions(store.getVersions());
    storeInfo.setAccessControlled(store.isAccessControlled());
    storeInfo.setIncrementalPushEnabled(store.isIncrementalPushEnabled());
    if (store.isHybrid()) {
      storeInfo.setHybridStoreConfig(store.getHybridStoreConfig());
    }
    storeInfo.setChunkingEnabled(store.isChunkingEnabled());
    storeInfo.setSingleGetRouterCacheEnabled(store.isSingleGetRouterCacheEnabled());
    storeInfo.setBatchGetRouterCacheEnabled(store.isBatchGetRouterCacheEnabled());
    storeInfo.setBatchGetLimit(store.getBatchGetLimit());
    storeInfo.setLargestUsedVersionNumber(store.getLargestUsedVersionNumber());
    storeInfo.setCompressionStrategy(store.getCompressionStrategy());
    storeInfo.setNumVersionsToPreserve(store.getNumVersionsToPreserve());
    storeInfo.setMigrating(store.isMigrating());
    storeInfo.setWriteComputationEnabled(store.isWriteComputationEnabled());
    storeInfo.setReadComputationEnabled(store.isReadComputationEnabled());

    return storeInfo;
  }
  /**
   * Store name.
   */
  private String name;
  /**
   * Owner of this store.
   */
  private String owner;
  /**
   * The number of version which is used currently.
   */
  private int currentVersion = 0;

  /**
   * The map represent the current versions in different colos.
   */
  private Map<String, Integer> coloToCurrentVersions;

  /**
   * Highest version number that has been claimed by an upstream (H2V) system which will create the corresponding kafka topic.
   */
  private int reservedVersion = 0;
  /**
   * Default partition count for all of versions in this store. Once first version is activated, the number will be
   * assigned.
   */
  private int partitionCount = 0;
  /**
   * If a store is enableStoreWrites, new version can not be created for it.
   */
  private boolean enableStoreWrites = true;
  /**
   * If a store is enableStoreReads, store has not version available to serve read requests.
   */
  private boolean enableStoreReads = true;
  /**
   * List of non-retired versions.
   */
  private List<Version> versions;

  /**
   * Maximum capacity a store version is able to have
   */
  private long storageQuotaInByte;
  /**
   * Quota for read request hit this store. Measurement is capacity unit.
   */
  private long readQuotaInCU;

  /**
   * Configurations for hybrid stores.
   */
  private HybridStoreConfig hybridStoreConfig;

  /**
   * Store-level ACL switch. When disabled, Venice Router should accept every request.
   */
  private boolean accessControlled = false;

  /**
   * Whether the chunking is enabled, and this is for large value store.
   */
  private boolean chunkingEnabled = false;

  /**
   * Whether cache is enabled in Router.
   */
  private boolean singleGetRouterCacheEnabled = false;

  /**
   * Whether batch-get cache is enabled in Router.
   */
  private boolean batchGetRouterCacheEnabled = false;

  /**
   * Batch get limit for current store.
   */
  private int batchGetLimit;

  /**
   * Largest used version number. Topics corresponding to store-versions equal to or lesser than this
   * version number will not trigger new OfflinePushJobs.
   */
  private int largestUsedVersionNumber;

  /**
   * a flag to see if the store supports incremental push or not
   */
  private boolean incrementalPushEnabled;

  /**
   * strategies used to compress/decompress Record's value
   */
  private CompressionStrategy compressionStrategy = CompressionStrategy.NO_OP;

  /**
   * How many versions this store preserve at most. By default it's 0 means we use the cluster level config to
   * determine how many version is preserved.
   */
  private int numVersionsToPreserve = NUM_VERSION_PRESERVE_NOT_SET;

  /**
   * Whether or not the store is in the process of migration.
   */
  private boolean migrating = false;

  /**
   * Whether or not write-path computation feature is enabled for this store
   */
  private boolean writeComputationEnabled = false;

  /**
   * Whether read-path computation is enabled for this store.
   */
  private boolean readComputationEnabled = false;

  public StoreInfo() {
  }

  /**
   * Store Name
   * @return
   */
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  /**
   * Store Owner
   * @return
   */
  public String getOwner() {
    return owner;
  }

  public void setOwner(String owner) {
    this.owner = owner;
  }

  /**
   * The version of the store which is currently being served
   * @return
   */
  public int getCurrentVersion() {
    return currentVersion;
  }

  public void setCurrentVersion(int currentVersion) {
    this.currentVersion = currentVersion;
  }

  public Map<String, Integer> getColoToCurrentVersions() {
    return coloToCurrentVersions;
  }

  public void setColoToCurrentVersions(Map<String, Integer> coloToCurrentVersions) {
    this.coloToCurrentVersions = coloToCurrentVersions;
  }

  /**
   * The highest version number that has been reserved.
   * Any component that did not reserve a version must create or reserve versions higher than this
   * @return
   */
  public int getReservedVersion() {
    return reservedVersion;
  }

  public void setReservedVersion(int reservedVersion) {
    this.reservedVersion = reservedVersion;
  }

  /**
   * The number of partitions for this store
   * @return
   */
  public int getPartitionCount() {
    return partitionCount;
  }

  public void setPartitionCount(int partitionCount) {
    this.partitionCount = partitionCount;
  }

  /**
   * Whether the store is enableStoreWrites, a enableStoreWrites store cannot have new versions pushed
   * @return
   */
  public boolean isEnableStoreWrites() {
    return enableStoreWrites;
  }

  public void setEnableStoreWrites(boolean enableStoreWrites) {
    this.enableStoreWrites = enableStoreWrites;
  }

  public boolean isEnableStoreReads() {
    return enableStoreReads;
  }

  public void setEnableStoreReads(boolean enableStoreReads) {
    this.enableStoreReads = enableStoreReads;
  }

  /**
   * List of available versions for this store
   * @return
   */
  public List<Version> getVersions() {
    return versions;
  }

  public void setVersions(List<Version> versions) {
    this.versions = versions;
  }

  public long getStorageQuotaInByte() {
    return storageQuotaInByte;
  }

  public void setStorageQuotaInByte(long storageQuotaInByte) {
    this.storageQuotaInByte = storageQuotaInByte;
  }

  public long getReadQuotaInCU() {
    return readQuotaInCU;
  }

  public void setReadQuotaInCU(long readQuotaInCU) {
    this.readQuotaInCU = readQuotaInCU;
  }

  public HybridStoreConfig getHybridStoreConfig() {
    return hybridStoreConfig;
  }

  public void setHybridStoreConfig(HybridStoreConfig hybridStoreConfig) {
    this.hybridStoreConfig = hybridStoreConfig;
  }

  public boolean isAccessControlled() {
    return accessControlled;
  }

  public void setAccessControlled(boolean accessControlled) {
    this.accessControlled = accessControlled;
  }

  public boolean isChunkingEnabled() {
    return chunkingEnabled;
  }

  public void setChunkingEnabled(boolean chunkingEnabled) {
    this.chunkingEnabled = chunkingEnabled;
  }

  public boolean isSingleGetRouterCacheEnabled() {
    return singleGetRouterCacheEnabled;
  }

  public void setSingleGetRouterCacheEnabled(boolean singleGetRouterCacheEnabled) {
    this.singleGetRouterCacheEnabled = singleGetRouterCacheEnabled;
  }

  public boolean isBatchGetRouterCacheEnabled() {
    return batchGetRouterCacheEnabled;
  }

  public void setBatchGetRouterCacheEnabled(boolean batchGetRouterCacheEnabled) {
    this.batchGetRouterCacheEnabled = batchGetRouterCacheEnabled;
  }

  public int getBatchGetLimit() {
    return batchGetLimit;
  }

  public void setBatchGetLimit(int batchGetLimit) {
    this.batchGetLimit = batchGetLimit;
  }

  public int getLargestUsedVersionNumber() {
    return largestUsedVersionNumber;
  }

  public void setLargestUsedVersionNumber(int largestUsedVersionNumber) {
    this.largestUsedVersionNumber = largestUsedVersionNumber;
  }

  public boolean isIncrementalPushEnabled() {
    return incrementalPushEnabled;
  }

  public void setIncrementalPushEnabled(boolean incrementalPushEnabled) {
    this.incrementalPushEnabled = incrementalPushEnabled;
  }

  public CompressionStrategy getCompressionStrategy() {
    return compressionStrategy;
  }

  public void setCompressionStrategy(CompressionStrategy compressionStrategy) {
    this.compressionStrategy = compressionStrategy;
  }

  public int getNumVersionsToPreserve() {
    return numVersionsToPreserve;
  }

  public void setNumVersionsToPreserve(int numVersionsToPreserve) {
    this.numVersionsToPreserve = numVersionsToPreserve;
  }

  public boolean isMigrating() {
    return migrating;
  }

  public void setMigrating(boolean migrating) {
    this.migrating = migrating;
  }

  public boolean isWriteComputationEnabled() {
    return writeComputationEnabled;
  }

  public void setWriteComputationEnabled(boolean writeComputationEnabled) {
    this.writeComputationEnabled = writeComputationEnabled;
  }

  public boolean isReadComputationEnabled() {
    return readComputationEnabled;
  }

  public void setReadComputationEnabled(boolean readComputationEnabled) {
    this.readComputationEnabled = readComputationEnabled;
  }
}
