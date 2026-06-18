package com.linkedin.venice.hadoop.snapshot;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.schema.GeneratedSchemaID;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


/**
 * A minimal, read-only {@link ReadOnlySchemaRepository} for the snapshot-at-T merge, populated once from the
 * controller's value / replication-metadata / derived schemas for a single store. Only the methods that
 * {@link SnapshotAtTRecordMerger} (via {@code MergeConflictResolver}) calls are implemented; the rest throw.
 */
public class SnapshotAtTSchemaRepository implements ReadOnlySchemaRepository {
  private final Map<Integer, SchemaEntry> valueSchemas;
  private final int latestValueSchemaId;
  private final Map<Long, RmdSchemaEntry> rmdSchemas;
  private final Map<Long, DerivedSchemaEntry> derivedSchemas;
  private final int rmdVersionId;

  private SnapshotAtTSchemaRepository(
      Map<Integer, SchemaEntry> valueSchemas,
      int latestValueSchemaId,
      Map<Long, RmdSchemaEntry> rmdSchemas,
      Map<Long, DerivedSchemaEntry> derivedSchemas,
      int rmdVersionId) {
    this.valueSchemas = valueSchemas;
    this.latestValueSchemaId = latestValueSchemaId;
    this.rmdSchemas = rmdSchemas;
    this.derivedSchemas = derivedSchemas;
    this.rmdVersionId = rmdVersionId;
  }

  /** The replication-metadata protocol version the store's RMD schemas are registered with. */
  public int getRmdVersionId() {
    return rmdVersionId;
  }

  public static SnapshotAtTSchemaRepository fromController(ControllerClient controllerClient, String storeName) {
    Map<Integer, SchemaEntry> valueSchemas = new HashMap<>();
    int latestValueSchemaId = -1;
    MultiSchemaResponse valueResponse = controllerClient.getAllValueSchema(storeName);
    if (valueResponse.isError()) {
      throw new VeniceException(
          "Failed to fetch value schemas for store " + storeName + ": " + valueResponse.getError());
    }
    for (MultiSchemaResponse.Schema schema: valueResponse.getSchemas()) {
      valueSchemas.put(schema.getId(), new SchemaEntry(schema.getId(), schema.getSchemaStr()));
      latestValueSchemaId = Math.max(latestValueSchemaId, schema.getId());
    }
    if (valueSchemas.isEmpty()) {
      throw new VeniceException(
          "No value schemas returned for store " + storeName + "; cannot run the snapshot-at-T merge.");
    }

    // RMD and derived schemas are required for correct conflict resolution / write-compute merges, so a fetch
    // error must abort the push rather than silently proceed with an empty schema set.
    Map<Long, RmdSchemaEntry> rmdSchemas = new HashMap<>();
    int rmdVersionId = -1;
    MultiSchemaResponse rmdResponse = controllerClient.getAllReplicationMetadataSchemas(storeName);
    if (rmdResponse.isError()) {
      throw new VeniceException(
          "Failed to fetch replication-metadata schemas for store " + storeName + ": " + rmdResponse.getError());
    }
    for (MultiSchemaResponse.Schema schema: rmdResponse.getSchemas()) {
      rmdSchemas.put(
          key(schema.getRmdValueSchemaId(), schema.getId()),
          new RmdSchemaEntry(schema.getRmdValueSchemaId(), schema.getId(), schema.getSchemaStr()));
      rmdVersionId = Math.max(rmdVersionId, schema.getId());
    }

    Map<Long, DerivedSchemaEntry> derivedSchemas = new HashMap<>();
    MultiSchemaResponse derivedResponse = controllerClient.getAllValueAndDerivedSchema(storeName);
    if (derivedResponse.isError()) {
      throw new VeniceException(
          "Failed to fetch derived schemas for store " + storeName + ": " + derivedResponse.getError());
    }
    for (MultiSchemaResponse.Schema schema: derivedResponse.getSchemas()) {
      if (schema.getDerivedSchemaId() > 0) {
        derivedSchemas.put(
            key(schema.getId(), schema.getDerivedSchemaId()),
            new DerivedSchemaEntry(schema.getId(), schema.getDerivedSchemaId(), schema.getSchemaStr()));
      }
    }

    return new SnapshotAtTSchemaRepository(valueSchemas, latestValueSchemaId, rmdSchemas, derivedSchemas, rmdVersionId);
  }

  private static long key(int valueSchemaId, int subId) {
    return (((long) valueSchemaId) << 32) | (subId & 0xffffffffL);
  }

  @Override
  public SchemaEntry getValueSchema(String storeName, int id) {
    return valueSchemas.get(id);
  }

  @Override
  public SchemaEntry getSupersetOrLatestValueSchema(String storeName) {
    return valueSchemas.get(latestValueSchemaId);
  }

  @Override
  public SchemaEntry getSupersetSchema(String storeName) {
    return valueSchemas.get(latestValueSchemaId);
  }

  @Override
  public Collection<SchemaEntry> getValueSchemas(String storeName) {
    return valueSchemas.values();
  }

  @Override
  public DerivedSchemaEntry getDerivedSchema(String storeName, int valueSchemaId, int writeComputeSchemaId) {
    return derivedSchemas.get(key(valueSchemaId, writeComputeSchemaId));
  }

  @Override
  public RmdSchemaEntry getReplicationMetadataSchema(
      String storeName,
      int valueSchemaId,
      int replicationMetadataVersionId) {
    return rmdSchemas.get(key(valueSchemaId, replicationMetadataVersionId));
  }

  @Override
  public boolean hasValueSchema(String storeName, int id) {
    return valueSchemas.containsKey(id);
  }

  @Override
  public SchemaEntry getKeySchema(String storeName) {
    throw new UnsupportedOperationException("getKeySchema is not supported by SnapshotAtTSchemaRepository");
  }

  @Override
  public int getValueSchemaId(String storeName, String valueSchemaStr) {
    throw new UnsupportedOperationException("getValueSchemaId is not supported by SnapshotAtTSchemaRepository");
  }

  @Override
  public GeneratedSchemaID getDerivedSchemaId(String storeName, String derivedSchemaStr) {
    throw new UnsupportedOperationException("getDerivedSchemaId is not supported by SnapshotAtTSchemaRepository");
  }

  @Override
  public Collection<DerivedSchemaEntry> getDerivedSchemas(String storeName) {
    return derivedSchemas.values();
  }

  @Override
  public DerivedSchemaEntry getLatestDerivedSchema(String storeName, int valueSchemaId) {
    throw new UnsupportedOperationException("getLatestDerivedSchema is not supported by SnapshotAtTSchemaRepository");
  }

  @Override
  public Collection<RmdSchemaEntry> getReplicationMetadataSchemas(String storeName) {
    return rmdSchemas.values();
  }

  @Override
  public void refresh() {
    // Schemas are fetched once at construction; nothing to refresh.
  }

  @Override
  public void clear() {
    // No-op.
  }
}
