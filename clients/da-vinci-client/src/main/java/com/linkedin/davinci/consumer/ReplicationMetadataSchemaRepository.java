package com.linkedin.davinci.consumer;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


public class ReplicationMetadataSchemaRepository {
  private final ControllerClient controllerClient;

  private List<RmdSchemaEntry> cachedReplicationMetadataSchemas = new ArrayList<>();

  public ReplicationMetadataSchemaRepository(ControllerClient controllerClient) {
    this.controllerClient = controllerClient;
  }

  public RmdSchemaEntry getReplicationMetadataSchemaById(String storeName, int replicationMetadataSchemaId) {
    if (cachedReplicationMetadataSchemas.size() < replicationMetadataSchemaId) {
      MultiSchemaResponse multiReplicationSchemaResponse = controllerClient.getAllReplicationMetadataSchemas(storeName);
      if (multiReplicationSchemaResponse.isError()) {
        throw new VeniceException(
            "Failed to get store replication info for store: " + storeName + " with error: "
                + multiReplicationSchemaResponse.getError());
      }
      cachedReplicationMetadataSchemas = Arrays.stream(multiReplicationSchemaResponse.getSchemas())
          .map(schema -> new RmdSchemaEntry(schema.getRmdValueSchemaId(), schema.getId(), schema.getSchemaStr()))
          .collect(Collectors.toList());
      if (cachedReplicationMetadataSchemas.size() < replicationMetadataSchemaId) {
        throw new VeniceException("No available store replication metadata schema for store: " + storeName);
      }
    }
    return cachedReplicationMetadataSchemas.get(replicationMetadataSchemaId - 1);
  }
}
