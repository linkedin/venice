package com.linkedin.venice.meta;

import com.linkedin.venice.systemstore.schemas.SystemStoreProperties;
import com.linkedin.venice.utils.AvroCompatibilityUtils;
import com.linkedin.venice.utils.AvroRecordUtils;
import java.util.List;
import java.util.stream.Collectors;


public class SystemStoreAttributesImpl implements SystemStoreAttributes {
  private final SystemStoreProperties dataModel;

  public SystemStoreAttributesImpl() {
    this(AvroRecordUtils.prefillAvroRecordWithDefaultValue(new SystemStoreProperties()));
  }

  SystemStoreAttributesImpl(SystemStoreProperties dataModel) {
    this.dataModel = dataModel;
  }

  @Override
  public SystemStoreProperties dataModel() {
    return this.dataModel;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SystemStoreAttributesImpl systemStoreAttributes = (SystemStoreAttributesImpl) o;
    return AvroCompatibilityUtils.compare(dataModel, systemStoreAttributes.dataModel);
  }

  @Override
  public int hashCode() {
    return this.dataModel.hashCode();
  }

  @Override
  public int getLargestUsedVersionNumber() {
    return this.dataModel.largestUsedVersionNumber;
  }

  @Override
  public void setLargestUsedVersionNumber(int largestUsedVersionNumber) {
    this.dataModel.largestUsedVersionNumber = largestUsedVersionNumber;
  }

  @Override
  public int getCurrentVersion() {
    return this.dataModel.currentVersion;
  }

  @Override
  public void setCurrentVersion(int currentVersion) {
    this.dataModel.currentVersion = currentVersion;
  }

  @Override
  public long getLatestVersionPromoteToCurrentTimestamp() {
    return this.dataModel.latestVersionPromoteToCurrentTimestamp;
  }

  @Override
  public void setLatestVersionPromoteToCurrentTimestamp(long timestamp) {
    this.dataModel.latestVersionPromoteToCurrentTimestamp = timestamp;
  }

  @Override
  public List<Version> getVersions() {
    return this.dataModel.versions.stream().map(v -> new VersionImpl(v)).collect(Collectors.toList());
  }

  @Override
  public void setVersions(List<Version> versions) {
    this.dataModel.versions = versions.stream().map(Version::dataModel).collect(Collectors.toList());
    ;
  }

  @Override
  public SystemStoreAttributes clone() {
    SystemStoreAttributes clone = new SystemStoreAttributesImpl();
    clone.setLargestUsedVersionNumber(getLargestUsedVersionNumber());
    clone.setCurrentVersion(getCurrentVersion());
    clone.setLatestVersionPromoteToCurrentTimestamp(getLatestVersionPromoteToCurrentTimestamp());
    clone.setVersions(getVersions().stream().map(v -> v.cloneVersion()).collect(Collectors.toList()));

    return clone;
  }
}
