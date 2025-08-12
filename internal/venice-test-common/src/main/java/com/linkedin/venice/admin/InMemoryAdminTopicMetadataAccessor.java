package com.linkedin.venice.admin;

import com.linkedin.venice.controller.AdminTopicMetadataAccessor;
import com.linkedin.venice.controller.kafka.consumer.AdminMetadata;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * In memory implementation of {@link AdminTopicMetadataAccessor}, should only be used for testing purposes.
 */
public class InMemoryAdminTopicMetadataAccessor extends AdminTopicMetadataAccessor {
  private static final Logger LOGGER = LogManager.getLogger(InMemoryAdminTopicMetadataAccessor.class);
  private AdminMetadata inMemoryMetadata = new AdminMetadata();

  @Override
  public void updateMetadata(String clusterName, AdminMetadata metadata) {
    // Convert AdminMetadata to legacy Map format for backward compatibility
    Map<String, Long> metadataMap = new HashMap<>();

    if (metadata.getExecutionId() != null) {
      metadataMap.put(AdminTopicMetadataAccessor.EXECUTION_ID_KEY, metadata.getExecutionId());
    }
    if (metadata.getOffset() != null) {
      metadataMap.put(AdminTopicMetadataAccessor.OFFSET_KEY, metadata.getOffset());
    }
    if (metadata.getUpstreamOffset() != null) {
      metadataMap.put(AdminTopicMetadataAccessor.UPSTREAM_OFFSET_KEY, metadata.getUpstreamOffset());
    }
    if (!metadata.getAdminOperationProtocolVersion().equals(UNDEFINED_VALUE)) {
      metadataMap.put(
          AdminTopicMetadataAccessor.ADMIN_OPERATION_PROTOCOL_VERSION_KEY,
          metadata.getAdminOperationProtocolVersion());
    }

    inMemoryMetadata = AdminMetadata.fromLegacyMap(metadataMap);
    LOGGER.info("Persisted admin topic metadata for cluster: {}, metadata: {}", clusterName, metadata);
  }

  @Override
  public AdminMetadata getMetadata(String clusterName) {
    return inMemoryMetadata;
  }
}
