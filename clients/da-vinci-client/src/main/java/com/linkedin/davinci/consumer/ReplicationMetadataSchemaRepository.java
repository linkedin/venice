package com.linkedin.davinci.consumer;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.utils.SparseConcurrentList;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;


public class ReplicationMetadataSchemaRepository {
  private final ControllerClient controllerClient;

  private final SparseConcurrentList<RmdSchemaEntry> cachedReplicationMetadataSchemas = new SparseConcurrentList<>();

  public ReplicationMetadataSchemaRepository(ControllerClient controllerClient) {
    this.controllerClient = controllerClient;
  }

  public RmdSchemaEntry getReplicationMetadataSchemaById(String storeName, int replicationMetadataSchemaId) {
    if (cachedReplicationMetadataSchemas.get(replicationMetadataSchemaId) == null) {
      MultiSchemaResponse multiReplicationSchemaResponse = null;
      int attempts = 0;
      while (true) {
        multiReplicationSchemaResponse = controllerClient.getAllReplicationMetadataSchemas(storeName);
        if (multiReplicationSchemaResponse.isError()) {
          if (attempts++ < 5) {
            Utils.sleep(Time.MS_PER_SECOND * (attempts + 1L));
            continue;
          }
          throw new VeniceException(
              "Failed to get store replication info for store: " + storeName + " with error: "
                  + multiReplicationSchemaResponse.getError());
        }
        break;
      }

      for (MultiSchemaResponse.Schema schema: multiReplicationSchemaResponse.getSchemas()) {
        cachedReplicationMetadataSchemas.computeIfAbsent(
            schema.getRmdValueSchemaId(),
            key -> new RmdSchemaEntry(schema.getRmdValueSchemaId(), schema.getId(), schema.getSchemaStr()));
      }
      if (cachedReplicationMetadataSchemas.get(replicationMetadataSchemaId) == null) {
        throw new VeniceException("No available store replication metadata schema for store: " + storeName);
      }
    }
    return cachedReplicationMetadataSchemas.get(replicationMetadataSchemaId);
  }
}
