package com.linkedin.venice.hadoop.snapshot;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * A serializable snapshot of a store's value / replication-metadata / derived schemas (as strings), fetched once
 * from the controller on the driver. Because it holds only strings and ids it can be Spark-broadcast to executors,
 * where {@link #buildMerger} reconstructs a {@link SnapshotAtTRecordMerger} per task — the merger itself wraps a
 * non-serializable {@code MergeConflictResolver}, so it cannot be shipped directly.
 *
 * <p>This is also the single place the controller schema fetch + validation lives;
 * {@link SnapshotAtTSchemaRepository#fromController} builds a repository from a bundle.
 */
public class SnapshotAtTSchemaBundle implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String storeName;
  private final Map<Integer, String> valueSchemas;
  private final int latestValueSchemaId;
  // Keyed by SnapshotAtTSchemaRepository.key(rmdValueSchemaId, protocolId) -> rmd schema string.
  private final Map<Long, String> rmdSchemas;
  // Keyed by SnapshotAtTSchemaRepository.key(valueSchemaId, derivedSchemaId) -> derived schema string.
  private final Map<Long, String> derivedSchemas;
  private final int rmdVersionId;

  SnapshotAtTSchemaBundle(
      String storeName,
      Map<Integer, String> valueSchemas,
      int latestValueSchemaId,
      Map<Long, String> rmdSchemas,
      Map<Long, String> derivedSchemas,
      int rmdVersionId) {
    this.storeName = storeName;
    this.valueSchemas = valueSchemas;
    this.latestValueSchemaId = latestValueSchemaId;
    this.rmdSchemas = rmdSchemas;
    this.derivedSchemas = derivedSchemas;
    this.rmdVersionId = rmdVersionId;
  }

  /** The replication-metadata protocol version the store's RMD schemas are registered with ({@code -1} if none). */
  public int getRmdVersionId() {
    return rmdVersionId;
  }

  public static SnapshotAtTSchemaBundle fromController(ControllerClient controllerClient, String storeName) {
    Map<Integer, String> valueSchemas = new HashMap<>();
    int latestValueSchemaId = -1;
    MultiSchemaResponse valueResponse = controllerClient.getAllValueSchema(storeName);
    if (valueResponse.isError()) {
      throw new VeniceException(
          "Failed to fetch value schemas for store " + storeName + ": " + valueResponse.getError());
    }
    for (MultiSchemaResponse.Schema schema: valueResponse.getSchemas()) {
      valueSchemas.put(schema.getId(), schema.getSchemaStr());
      latestValueSchemaId = Math.max(latestValueSchemaId, schema.getId());
    }
    if (valueSchemas.isEmpty()) {
      throw new VeniceException(
          "No value schemas returned for store " + storeName + "; cannot run the snapshot-at-T merge.");
    }

    // RMD and derived schemas are required for correct conflict resolution / write-compute merges, so a fetch
    // error must abort the push rather than silently proceed with an empty schema set.
    Map<Long, String> rmdSchemas = new HashMap<>();
    int rmdVersionId = -1;
    MultiSchemaResponse rmdResponse = controllerClient.getAllReplicationMetadataSchemas(storeName);
    if (rmdResponse.isError()) {
      throw new VeniceException(
          "Failed to fetch replication-metadata schemas for store " + storeName + ": " + rmdResponse.getError());
    }
    for (MultiSchemaResponse.Schema schema: rmdResponse.getSchemas()) {
      rmdSchemas
          .put(SnapshotAtTSchemaRepository.key(schema.getRmdValueSchemaId(), schema.getId()), schema.getSchemaStr());
      rmdVersionId = Math.max(rmdVersionId, schema.getId());
    }

    Map<Long, String> derivedSchemas = new HashMap<>();
    MultiSchemaResponse derivedResponse = controllerClient.getAllValueAndDerivedSchema(storeName);
    if (derivedResponse.isError()) {
      throw new VeniceException(
          "Failed to fetch derived schemas for store " + storeName + ": " + derivedResponse.getError());
    }
    for (MultiSchemaResponse.Schema schema: derivedResponse.getSchemas()) {
      if (schema.getDerivedSchemaId() > 0) {
        derivedSchemas
            .put(SnapshotAtTSchemaRepository.key(schema.getId(), schema.getDerivedSchemaId()), schema.getSchemaStr());
      }
    }

    return new SnapshotAtTSchemaBundle(
        storeName,
        valueSchemas,
        latestValueSchemaId,
        rmdSchemas,
        derivedSchemas,
        rmdVersionId);
  }

  /** Parse the bundled schema strings into a read-only repository (called on the driver and on each executor). */
  public SnapshotAtTSchemaRepository buildRepository() {
    Map<Integer, SchemaEntry> valueEntries = new HashMap<>();
    for (Map.Entry<Integer, String> e: valueSchemas.entrySet()) {
      valueEntries.put(e.getKey(), new SchemaEntry(e.getKey(), e.getValue()));
    }
    Map<Long, RmdSchemaEntry> rmdEntries = new HashMap<>();
    for (Map.Entry<Long, String> e: rmdSchemas.entrySet()) {
      int rmdValueSchemaId = SnapshotAtTSchemaRepository.keyHighBits(e.getKey());
      int protocolId = SnapshotAtTSchemaRepository.keyLowBits(e.getKey());
      rmdEntries.put(e.getKey(), new RmdSchemaEntry(rmdValueSchemaId, protocolId, e.getValue()));
    }
    Map<Long, DerivedSchemaEntry> derivedEntries = new HashMap<>();
    for (Map.Entry<Long, String> e: derivedSchemas.entrySet()) {
      int valueSchemaId = SnapshotAtTSchemaRepository.keyHighBits(e.getKey());
      int derivedSchemaId = SnapshotAtTSchemaRepository.keyLowBits(e.getKey());
      derivedEntries.put(e.getKey(), new DerivedSchemaEntry(valueSchemaId, derivedSchemaId, e.getValue()));
    }
    return new SnapshotAtTSchemaRepository(valueEntries, latestValueSchemaId, rmdEntries, derivedEntries, rmdVersionId);
  }

  /**
   * Build a {@link SnapshotAtTRecordMerger} from this bundle. Safe to call on an executor (parses the bundled
   * schema strings; no controller access).
   */
  public SnapshotAtTRecordMerger buildMerger(int rmdProtocolVersion, boolean rmdUseFieldLevelTimestamp) {
    return new SnapshotAtTRecordMerger(buildRepository(), storeName, rmdProtocolVersion, rmdUseFieldLevelTimestamp);
  }

  /** Visible for testing: build a bundle directly from schema strings without a controller. */
  static SnapshotAtTSchemaBundle of(
      String storeName,
      Map<Integer, String> valueSchemas,
      Map<Long, String> rmdSchemas,
      Map<Long, String> derivedSchemas,
      int rmdVersionId) {
    int latestValueSchemaId = -1;
    List<Integer> ids = new ArrayList<>(valueSchemas.keySet());
    for (int id: ids) {
      latestValueSchemaId = Math.max(latestValueSchemaId, id);
    }
    return new SnapshotAtTSchemaBundle(
        storeName,
        valueSchemas,
        latestValueSchemaId,
        rmdSchemas,
        derivedSchemas,
        rmdVersionId);
  }
}
