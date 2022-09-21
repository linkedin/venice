package com.linkedin.venice.admin;

import com.linkedin.venice.controller.AdminTopicMetadataAccessor;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * In memory implementation of {@link AdminTopicMetadataAccessor}, should only be used for testing purposes.
 */
public class InMemoryAdminTopicMetadataAccessor extends AdminTopicMetadataAccessor {
  private static final Logger LOGGER = LogManager.getLogger(InMemoryAdminTopicMetadataAccessor.class);
  private Map<String, Long> inMemoryMetadata = new HashMap<>();

  @Override
  public void updateMetadata(String clusterName, Map<String, Long> metadata) {
    inMemoryMetadata = metadata;
    LOGGER.info("Persisted admin topic metadata map for cluster: {}, map: {}", clusterName, metadata);
  }

  @Override
  public Map<String, Long> getMetadata(String clusterName) {
    return inMemoryMetadata;
  }
}
