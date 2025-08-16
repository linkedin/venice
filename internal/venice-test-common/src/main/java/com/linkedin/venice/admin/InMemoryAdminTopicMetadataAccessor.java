package com.linkedin.venice.admin;

import com.linkedin.venice.controller.AdminTopicMetadataAccessor;
import com.linkedin.venice.controller.kafka.consumer.AdminMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * In memory implementation of {@link AdminTopicMetadataAccessor}, should only be used for testing purposes.
 */
public class InMemoryAdminTopicMetadataAccessor extends AdminTopicMetadataAccessor {
  private static final Logger LOGGER = LogManager.getLogger(InMemoryAdminTopicMetadataAccessor.class);
  private final AdminMetadata inMemoryMetadata = new AdminMetadata();

  @Override
  public void updateMetadata(String clusterName, AdminMetadata metadata) {
    if (metadata.getExecutionId() != null) {
      inMemoryMetadata.setExecutionId(metadata.getExecutionId());
    }
    if (metadata.getOffset() != null) {
      inMemoryMetadata.setOffset(metadata.getOffset());
    }
    if (metadata.getUpstreamOffset() != null) {
      inMemoryMetadata.setUpstreamOffset(metadata.getUpstreamOffset());
    }
    if (!metadata.getAdminOperationProtocolVersion().equals(UNDEFINED_VALUE)) {
      inMemoryMetadata.setAdminOperationProtocolVersion(metadata.getAdminOperationProtocolVersion());
    }

    LOGGER.info("Persisted admin topic metadata for cluster: {}, metadata: {}", clusterName, metadata);
  }

  @Override
  public AdminMetadata getMetadata(String clusterName) {
    return inMemoryMetadata;
  }
}
