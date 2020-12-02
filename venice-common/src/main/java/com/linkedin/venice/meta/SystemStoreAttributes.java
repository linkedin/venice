package com.linkedin.venice.meta;

import com.linkedin.venice.systemstore.schemas.SystemStoreProperties;
import java.util.List;
import java.util.stream.Collectors;


public class SystemStoreAttributes implements DataModelBackedStructure<SystemStoreProperties> {
  private final SystemStoreProperties dataModel;

  public SystemStoreAttributes() {
    this(Store.prefillAvroRecordWithDefaultValue(new SystemStoreProperties()));
  }

  SystemStoreAttributes(SystemStoreProperties dataModel) {
    this.dataModel = dataModel;
  }

  @Override
  public SystemStoreProperties dataModel() {
    return this.dataModel;
  }

  public int getLargestUsedVersionNumber() {
    return this.dataModel.largestUsedVersionNumber;
  }

  public void setLargestUsedVersionNumber(int largestUsedVersionNumber) {
    this.dataModel.largestUsedVersionNumber = largestUsedVersionNumber;
  }

  public int getCurrentVersion() {
    return this.dataModel.currentVersion;
  }

  public void setCurrentVersion(int currentVersion) {
    this.dataModel.currentVersion = currentVersion;
  }

  public long getLatestVersionPromoteToCurrentTimestamp() {
    return this.dataModel.latestVersionPromoteToCurrentTimestamp;
  }

  public void setLatestVersionPromoteToCurrentTimestamp(long timestamp) {
    this.dataModel.latestVersionPromoteToCurrentTimestamp = timestamp;
  }

  public List<Version> getVersions() {
    return this.dataModel.versions.stream().map(v -> new Version(v)).collect(Collectors.toList());
  }

  public void setVersions(List<Version> versions) {
    this.dataModel.versions = versions.stream().map(Version::dataModel).collect(Collectors.toList());;
  }

  public SystemStoreAttributes clone() {
    SystemStoreAttributes clone = new SystemStoreAttributes();
    clone.setLargestUsedVersionNumber(getLargestUsedVersionNumber());
    clone.setCurrentVersion(getCurrentVersion());
    clone.setLatestVersionPromoteToCurrentTimestamp(getLatestVersionPromoteToCurrentTimestamp());
    clone.setVersions(getVersions().stream().map(v -> v.cloneVersion()).collect(Collectors.toList()));

    return clone;
  }
}
