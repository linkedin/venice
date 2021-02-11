package com.linkedin.venice.admin;

import com.linkedin.venice.controller.AdminTopicMetadataAccessor;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;


/**
 * In memory implementation of {@link AdminTopicMetadataAccessor}, should only be used for testing purposes.
 */
public class InMemoryAdminTopicMetadataAccssor extends AdminTopicMetadataAccessor {
  private static final Logger logger = Logger.getLogger(InMemoryAdminTopicMetadataAccssor.class);
  private Map<String, Long> inMemoryMetadata = new HashMap<>();

  @Override
  public void updateMetadata(String clusterName, Map<String, Long> metadata) {
    inMemoryMetadata = metadata;
    logger.info("Persisted admin topic metadata map for cluster: " + clusterName + ", map: " + metadata);
  }

  @Override
  public Map<String, Long> getMetadata(String clusterName) {
    return inMemoryMetadata;
  }
}
