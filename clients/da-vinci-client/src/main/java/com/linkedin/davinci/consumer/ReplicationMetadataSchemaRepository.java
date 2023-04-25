package com.linkedin.davinci.consumer;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse.Schema;
import com.linkedin.venice.exceptions.VeniceException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


public class ReplicationMetadataSchemaRepository {
  private ControllerClient controllerClient;

  private List<Schema> cachedReplicationMetadataSchemas = new ArrayList<>();

  public ReplicationMetadataSchemaRepository(ControllerClient controllerClient) {
    this.controllerClient = controllerClient;
  }

  public Schema getReplicationMetadataSchemaById(String storeName, int replicationMetadataSchemaId) {
    if (cachedReplicationMetadataSchemas.size() < replicationMetadataSchemaId) {
      MultiSchemaResponse multiReplicationSchemaResponse = controllerClient.getAllReplicationMetadataSchemas(storeName);
      if (multiReplicationSchemaResponse.isError()) {
        throw new VeniceException(
            "Failed to get store replication info for store: " + storeName + " with error: "
                + multiReplicationSchemaResponse.getError());
      }
      cachedReplicationMetadataSchemas =
          Arrays.stream(multiReplicationSchemaResponse.getSchemas()).collect(Collectors.toList());
      if (cachedReplicationMetadataSchemas.size() < replicationMetadataSchemaId) {
        throw new VeniceException("No available store replication metadata schema for store: " + storeName);
      }
    }
    return cachedReplicationMetadataSchemas.get(replicationMetadataSchemaId - 1);
  }
}
