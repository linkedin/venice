package com.linkedin.venice.helix;

import com.linkedin.venice.meta.VeniceSerializer;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.rmd.RmdSchemaEntry;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.PathResourceRegistry;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class HelixSchemaAccessor {
  private static final Logger logger = LogManager.getLogger(HelixSchemaAccessor.class);

  private static final int DEFAULT_ZK_REFRESH_ATTEMPTS = 3;
  private static final long DEFAULT_ZK_REFRESH_INTERVAL = TimeUnit.SECONDS.toMillis(10);

  // Key schema path name
  private static final String KEY_SCHEMA_PATH = "key-schema";
  // Value schema path name
  private static final String VALUE_SCHEMA_PATH = "value-schema";
  // Derived schema path name
  private static final String DERIVED_SCHEMA_PATH = "derived-schema";
  static final String MULTIPART_SCHEMA_VERSION_DELIMITER = "-";
  // Key schema id, can only be '1' since Venice only maintains one single key schema per store.
  static final String KEY_SCHEMA_ID = "1";

  public static final int VALUE_SCHEMA_STARTING_ID = 1;

  // Replication metadata schema path name. The value still uses "timestamp" for backward compatibility
  private static final String REPLICATION_METADATA_SCHEMA_PATH = "timestamp-metadata-schema";

  private final ZkBaseDataAccessor<SchemaEntry> schemaAccessor;
  private final ZkBaseDataAccessor<DerivedSchemaEntry> derivedSchemaAccessor;
  private final ZkBaseDataAccessor<RmdSchemaEntry> replicationMetadataSchemaAccessor;

  // Venice cluster name
  private final String clusterName;

  private final int refreshAttemptsForZkReconnect;
  private final long refreshIntervalForZkReconnectInMs;

  public HelixSchemaAccessor(ZkClient zkClient, HelixAdapterSerializer helixAdapterSerializer, String clusterName) {
    this(zkClient, helixAdapterSerializer, clusterName, DEFAULT_ZK_REFRESH_ATTEMPTS, DEFAULT_ZK_REFRESH_INTERVAL);
  }

  public HelixSchemaAccessor(
      ZkClient zkClient,
      HelixAdapterSerializer helixAdapterSerializer,
      String clusterName,
      int refreshAttemptsForZkReconnect,
      long refreshIntervalForZkReconnectInMs) {
    this.clusterName = clusterName;

    this.refreshAttemptsForZkReconnect = refreshAttemptsForZkReconnect;
    this.refreshIntervalForZkReconnectInMs = refreshIntervalForZkReconnectInMs;

    registerSerializerForSchema(zkClient, helixAdapterSerializer);
    schemaAccessor = new ZkBaseDataAccessor<>(zkClient);
    derivedSchemaAccessor = new ZkBaseDataAccessor<>(zkClient);
    replicationMetadataSchemaAccessor = new ZkBaseDataAccessor<>(zkClient);
  }

  private void registerSerializerForSchema(ZkClient zkClient, HelixAdapterSerializer adapter) {
    // Register schema serializer
    String keySchemaPath = getKeySchemaPath(PathResourceRegistry.WILDCARD_MATCH_ANY);
    String valueSchemaPath =
        getValueSchemaPath(PathResourceRegistry.WILDCARD_MATCH_ANY, PathResourceRegistry.WILDCARD_MATCH_ANY);
    String derivedSchemaPath = getDerivedSchemaParentPath(PathResourceRegistry.WILDCARD_MATCH_ANY).append("/")
        .append(PathResourceRegistry.WILDCARD_MATCH_ANY)
        .toString();
    String replicationMetadataSchemaPath =
        getReplicationMetadataSchemaParentPath(PathResourceRegistry.WILDCARD_MATCH_ANY).append("/")
            .append(PathResourceRegistry.WILDCARD_MATCH_ANY)
            .toString();
    VeniceSerializer<SchemaEntry> serializer = new SchemaEntrySerializer();
    adapter.registerSerializer(keySchemaPath, serializer);
    adapter.registerSerializer(valueSchemaPath, serializer);
    adapter.registerSerializer(derivedSchemaPath, new DerivedSchemaEntrySerializer());
    adapter.registerSerializer(replicationMetadataSchemaPath, new ReplicationMetadataSchemaEntrySerializer());
    zkClient.setZkSerializer(adapter);
  }

  public SchemaEntry getKeySchema(String storeName) {
    return schemaAccessor.get(getKeySchemaPath(storeName), null, AccessOption.PERSISTENT);
  }

  public SchemaEntry getValueSchema(String storeName, String id) {
    return schemaAccessor.get(getValueSchemaPath(storeName, id), null, AccessOption.PERSISTENT);
  }

  public List<SchemaEntry> getAllValueSchemas(String storeName) {
    return HelixUtils.getChildren(
        schemaAccessor,
        getValueSchemaParentPath(storeName).toString(),
        refreshAttemptsForZkReconnect,
        refreshIntervalForZkReconnectInMs);
  }

  public DerivedSchemaEntry getDerivedSchema(String storeName, String derivedSchemaIdPair) {
    return derivedSchemaAccessor
        .get(getDerivedSchemaPath(storeName, derivedSchemaIdPair), null, AccessOption.PERSISTENT);
  }

  public List<DerivedSchemaEntry> getAllDerivedSchemas(String storeName) {
    return HelixUtils.getChildren(
        derivedSchemaAccessor,
        getDerivedSchemaParentPath(storeName).toString(),
        refreshAttemptsForZkReconnect,
        refreshIntervalForZkReconnectInMs);
  }

  public void createKeySchema(String storeName, SchemaEntry schemaEntry) {
    HelixUtils.create(schemaAccessor, getKeySchemaPath(storeName), schemaEntry);
    logger.info("Set up key schema: {} for store: {}.", schemaEntry, storeName);
  }

  public void addValueSchema(String storeName, SchemaEntry schemaEntry) {
    HelixUtils.create(schemaAccessor, getValueSchemaPath(storeName, String.valueOf(schemaEntry.getId())), schemaEntry);
    logger.info("Added value schema: {} for store: {}.", schemaEntry, storeName);
  }

  public void addDerivedSchema(String storeName, DerivedSchemaEntry derivedSchemaEntry) {
    HelixUtils.create(
        schemaAccessor,
        getDerivedSchemaPath(
            storeName,
            String.valueOf(derivedSchemaEntry.getValueSchemaID()),
            String.valueOf(derivedSchemaEntry.getId())),
        derivedSchemaEntry);
    logger.info("Added derived schema: {} for store: {}.", derivedSchemaEntry, storeName);
  }

  public void removeDerivedSchema(String storeName, String derivedSchemaIdPair) {
    HelixUtils.remove(schemaAccessor, getDerivedSchemaPath(storeName, derivedSchemaIdPair));
    logger.info("Removed derived schema for store: {} derived schema id pair: {}.", storeName, derivedSchemaIdPair);
  }

  public void subscribeKeySchemaCreationChange(String storeName, IZkChildListener childListener) {
    schemaAccessor.subscribeChildChanges(getKeySchemaParentPath(storeName).toString(), childListener);
    logger.debug("Subscribe key schema child changes for store: {}.", storeName);
  }

  public void unsubscribeKeySchemaCreationChange(String storeName, IZkChildListener childListener) {
    schemaAccessor.unsubscribeChildChanges(getKeySchemaParentPath(storeName).toString(), childListener);
    logger.debug("Unsubscribe key schema child changes for store: {}.", storeName);
  }

  public void subscribeValueSchemaCreationChange(String storeName, IZkChildListener childListener) {
    schemaAccessor.subscribeChildChanges(getValueSchemaParentPath(storeName).toString(), childListener);
    logger.debug("Subscribe value schema child changes for store: {}.", storeName);
  }

  public void unsubscribeValueSchemaCreationChange(String storeName, IZkChildListener childListener) {
    schemaAccessor.unsubscribeChildChanges(getValueSchemaParentPath(storeName).toString(), childListener);
    logger.debug("Unsubscribe value schema child changes for store: {}.", storeName);
  }

  public void subscribeDerivedSchemaCreationChange(String storeName, IZkChildListener childListener) {
    derivedSchemaAccessor.subscribeChildChanges(getDerivedSchemaParentPath(storeName).toString(), childListener);
    logger.debug("Subscribe derived schema child changes for store: {}.", storeName);
  }

  public void unsubscribeDerivedSchemaCreationChanges(String storeName, IZkChildListener childListener) {
    derivedSchemaAccessor.unsubscribeChildChanges(getDerivedSchemaParentPath(storeName).toString(), childListener);
    logger.debug("Unsubscribe derived schema child changes for store: {}.", storeName);
  }

  protected StringBuilder getStorePath(String storeName) {
    StringBuilder sb = new StringBuilder(HelixUtils.getHelixClusterZkPath(clusterName));
    sb.append(HelixReadOnlyStoreRepository.STORE_REPOSITORY_PATH).append("/").append(storeName).append("/");
    return sb;
  }

  /**
   * Get absolute key schema parent path for a given store
   */
  StringBuilder getKeySchemaParentPath(String storeName) {
    return getStorePath(storeName).append(KEY_SCHEMA_PATH);
  }

  /**
   * Get absolute key schema path for a given store
   */
  String getKeySchemaPath(String storeName) {
    return getKeySchemaParentPath(storeName).append("/").append(KEY_SCHEMA_ID).toString();
  }

  /**
   * Get absolute value schema parent path for a given store
   */
  StringBuilder getValueSchemaParentPath(String storeName) {
    return getStorePath(storeName).append(VALUE_SCHEMA_PATH);
  }

  /**
   * Get absolute value schema path for a given store and schema id
   */
  String getValueSchemaPath(String storeName, String valueSchemaId) {
    return getValueSchemaParentPath(storeName).append("/").append(valueSchemaId).toString();
  }

  StringBuilder getDerivedSchemaParentPath(String storeName) {
    return getStorePath(storeName).append(DERIVED_SCHEMA_PATH);
  }

  String getDerivedSchemaPath(String storeName, String valueSchemaId, String derivedSchemaId) {
    return getDerivedSchemaParentPath(storeName).append("/")
        .append(valueSchemaId)
        .append(MULTIPART_SCHEMA_VERSION_DELIMITER)
        .append(derivedSchemaId)
        .toString();
  }

  String getDerivedSchemaPath(String storeName, String derivedSchemaIdPair) {
    return getDerivedSchemaParentPath(storeName).append("/").append(derivedSchemaIdPair).toString();
  }

  public RmdSchemaEntry getReplicationMetadataSchema(String storeName, String replicationMetadataVersionIdPair) {
    return replicationMetadataSchemaAccessor.get(
        getReplicationMetadataSchemaPath(storeName, replicationMetadataVersionIdPair),
        null,
        AccessOption.PERSISTENT);
  }

  public List<RmdSchemaEntry> getAllReplicationMetadataSchemas(String storeName) {
    return HelixUtils.getChildren(
        replicationMetadataSchemaAccessor,
        getReplicationMetadataSchemaParentPath(storeName).toString(),
        refreshAttemptsForZkReconnect,
        refreshIntervalForZkReconnectInMs);
  }

  public void addReplicationMetadataSchema(String storeName, RmdSchemaEntry rmdSchemaEntry) {
    HelixUtils.create(
        replicationMetadataSchemaAccessor,
        getReplicationMetadataSchemaPath(
            storeName,
            String.valueOf(rmdSchemaEntry.getValueSchemaID()),
            String.valueOf(rmdSchemaEntry.getId())),
        rmdSchemaEntry);
    logger.info("Added replication metadata schema: {} for store: {}.", rmdSchemaEntry, storeName);
  }

  public void subscribeReplicationMetadataSchemaCreationChange(String storeName, IZkChildListener childListener) {
    replicationMetadataSchemaAccessor
        .subscribeChildChanges(getReplicationMetadataSchemaParentPath(storeName).toString(), childListener);
    logger.debug("Subscribe replication metadata schema child changes for store: {}", storeName);
  }

  public void unsubscribeReplicationMetadataSchemaCreationChanges(String storeName, IZkChildListener childListener) {
    replicationMetadataSchemaAccessor
        .unsubscribeChildChanges(getReplicationMetadataSchemaParentPath(storeName).toString(), childListener);
    logger.debug("Unsubscribe replication metadata schema child changes for store: {}.", storeName);
  }

  StringBuilder getReplicationMetadataSchemaParentPath(String storeName) {
    return getStorePath(storeName).append(REPLICATION_METADATA_SCHEMA_PATH);
  }

  String getReplicationMetadataSchemaPath(String storeName, String valueSchemaId, String replicationMetadataVersionId) {
    return getReplicationMetadataSchemaParentPath(storeName).append("/")
        .append(valueSchemaId)
        .append(MULTIPART_SCHEMA_VERSION_DELIMITER)
        .append(replicationMetadataVersionId)
        .toString();
  }

  String getReplicationMetadataSchemaPath(String storeName, String replicationMetadataVersionIdPair) {
    return getReplicationMetadataSchemaParentPath(storeName).append("/")
        .append(replicationMetadataVersionIdPair)
        .toString();
  }

  public void removeValueSchema(String storeName, int schemaID) {
    if (!HelixUtils.exists(schemaAccessor, getValueSchemaPath(storeName, String.valueOf(schemaID)))) {
      logger.info("Value schema with id : " + schemaID + " does not exist for store: " + storeName);
      return;
    }
    HelixUtils.remove(schemaAccessor, getValueSchemaPath(storeName, String.valueOf(schemaID)));
    logger.info("Removed value schema: " + schemaID + " for store: " + storeName);
  }
}
