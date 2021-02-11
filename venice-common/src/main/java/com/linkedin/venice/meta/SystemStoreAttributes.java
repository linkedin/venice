package com.linkedin.venice.meta;

import com.linkedin.venice.systemstore.schemas.SystemStoreProperties;
import com.linkedin.venice.utils.AvroCompatibilityUtils;
import java.util.List;
import java.util.stream.Collectors;
import org.codehaus.jackson.map.annotate.JsonDeserialize;


@JsonDeserialize(as = SystemStoreAttributesImpl.class)
@com.fasterxml.jackson.databind.annotation.JsonDeserialize(as = SystemStoreAttributesImpl.class)
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
