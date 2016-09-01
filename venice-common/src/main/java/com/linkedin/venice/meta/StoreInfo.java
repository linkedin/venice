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
    storeInfo.setReservedVersion(store.getReservedVersion());
    storeInfo.setPartitionCount(store.getPartitionCount());
    storeInfo.setPaused(store.isPaused());
    storeInfo.setVersions(store.getVersions());
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
   * If a store is paused, new version can not be created for it.
   */
  private boolean paused = false;
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
   * Whether the store is paused, a paused store cannot have new versions pushed
   * @return
   */
  public boolean isPaused() {
    return paused;
  }

  public void setPaused(boolean paused) {
    this.paused = paused;
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
}
