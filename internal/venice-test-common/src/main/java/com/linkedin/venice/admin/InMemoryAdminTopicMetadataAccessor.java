package com.linkedin.venice.admin;

import com.linkedin.venice.controller.AdminTopicMetadataAccessor;
import com.linkedin.venice.controller.kafka.consumer.AdminMetadata;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import com.linkedin.venice.pubsub.api.PubSubSymbolicPosition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * In memory implementation of {@link AdminTopicMetadataAccessor}, should only be used for testing purposes.
 */
public class InMemoryAdminTopicMetadataAccessor extends AdminTopicMetadataAccessor {
  private static final Logger LOGGER = LogManager.getLogger(InMemoryAdminTopicMetadataAccessor.class);
  private final AdminMetadata inMemoryMetadata = new AdminMetadata();

  @Override
  public void updateMetadata(String clusterName, AdminMetadata metadataDelta) {
    PubSubPosition newPosition = metadataDelta.getPosition();
    PubSubPosition newUpstreamPosition = metadataDelta.getUpstreamPosition();
    if (metadataDelta.getExecutionId() != null) {
      inMemoryMetadata.setExecutionId(metadataDelta.getExecutionId());
    }
    if (metadataDelta.getOffset() != null) {
      inMemoryMetadata.setOffset(metadataDelta.getOffset());
    }
    if (metadataDelta.getUpstreamOffset() != null) {
      inMemoryMetadata.setUpstreamOffset(metadataDelta.getUpstreamOffset());
    }
    if (!metadataDelta.getAdminOperationProtocolVersion().equals(UNDEFINED_VALUE)) {
      inMemoryMetadata.setAdminOperationProtocolVersion(metadataDelta.getAdminOperationProtocolVersion());
    }
    if (!newPosition.equals(PubSubSymbolicPosition.EARLIEST)) {
      inMemoryMetadata.setPubSubPosition(newPosition);
    }
    if (!newUpstreamPosition.equals(PubSubSymbolicPosition.EARLIEST)) {
      inMemoryMetadata.setPubSubPosition(newUpstreamPosition);
    }

    LOGGER.info("Persisted admin topic metadata for cluster: {}, metadata: {}", clusterName, metadataDelta);
  }

  @Override
  public AdminMetadata getMetadata(String clusterName) {
    return inMemoryMetadata;
  }
}
