package com.linkedin.venice.meta;

import java.util.List;


/**
 * Json-serializable class for sending store information to the controller client
 */
public class StoreInfo {
  public static StoreInfo fromStore(Store store){
    StoreInfo storeInfo = new StoreInfo();
    storeInfo.setName(store.getName());
    storeInfo.setOwner(store.getOwner());
    storeInfo.setCurrentVersion(store.getCurrentVersion());
    storeInfo.setPartitionCount(store.getPartitionCount());
    storeInfo.setEnableStoreWrites(store.isEnableWrites());
    storeInfo.setEnableStoreReads(store.isEnableReads());
    storeInfo.setVersions(store.getVersions());
    storeInfo.setPrinciples(String.join(",", store.getPrinciples()));
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
   * A SSL Identification used to certificate the access to this store.
   */
  private String principles;
  /**
   * List of non-retired versions.
   */
  private List<Version> versions;

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

  public String getPrinciples() {
    return principles;
  }

  public void setPrinciples(String principles) {
    this.principles = principles;
  }
}
