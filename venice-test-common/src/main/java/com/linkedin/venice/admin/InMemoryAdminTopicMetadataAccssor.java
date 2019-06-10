package com.linkedin.venice.admin;

import com.linkedin.venice.controller.AdminTopicMetadataAccessor;
import java.util.HashMap;
import java.util.Map;


/**
 * In memory implementation of {@link AdminTopicMetadataAccessor}, should only be used for testing purposes.
 */
public class InMemoryAdminTopicMetadataAccssor extends AdminTopicMetadataAccessor {
  private Map<String, Long> inMemoryMetadata = new HashMap<>();

  @Override
  public void updateMetadata(String clusterName, Map<String, Long> metadata) {
    inMemoryMetadata = metadata;
  }

  @Override
  public Map<String, Long> getMetadata(String clusterName) {
    return inMemoryMetadata;
  }
}
