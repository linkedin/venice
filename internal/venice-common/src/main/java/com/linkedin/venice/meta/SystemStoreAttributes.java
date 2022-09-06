package com.linkedin.venice.meta;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.linkedin.venice.systemstore.schemas.SystemStoreProperties;
import java.util.List;


@JsonDeserialize(as = SystemStoreAttributesImpl.class)
public interface SystemStoreAttributes extends DataModelBackedStructure<SystemStoreProperties> {
  int getLargestUsedVersionNumber();

  void setLargestUsedVersionNumber(int largestUsedVersionNumber);

  int getCurrentVersion();

  void setCurrentVersion(int currentVersion);

  long getLatestVersionPromoteToCurrentTimestamp();

  void setLatestVersionPromoteToCurrentTimestamp(long timestamp);

  List<Version> getVersions();

  void setVersions(List<Version> versions);

  SystemStoreAttributes clone();
}
