package com.linkedin.venice.helix;

import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.meta.VeniceSerializer;
import com.linkedin.venice.schema.MetadataSchemaEntry;
import com.linkedin.venice.schema.DerivedSchemaEntry;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.PathResourceRegistry;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.helix.zookeeper.zkclient.IZkChildListener;
import org.apache.log4j.Logger;


public class HelixSchemaAccessor {
  protected final Logger logger = Logger.getLogger(getClass().getSimpleName());

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

  // Metadata schema path name
  private static final String METADATA_SCHEMA_PATH = "metadata-schema";

  private final ZkBaseDataAccessor<SchemaEntry> schemaAccessor;
  private final ZkBaseDataAccessor<DerivedSchemaEntry> derivedSchemaAccessor;
  private final ZkBaseDataAccessor<MetadataSchemaEntry> metadataSchemaAccessor;


  // Venice cluster name
  private final String clusterName;

  private final int refreshAttemptsForZkReconnect;
  private final long refreshIntervalForZkReconnectInMs;

  public HelixSchemaAccessor(ZkClient zkClient, HelixAdapterSerializer helixAdapterSerializer, String clusterName) {
    this(zkClient, helixAdapterSerializer, clusterName, DEFAULT_ZK_REFRESH_ATTEMPTS, DEFAULT_ZK_REFRESH_INTERVAL);
  }

  public HelixSchemaAccessor(ZkClient zkClient, HelixAdapterSerializer helixAdapterSerializer, String clusterName,
      int refreshAttemptsForZkReconnect, long refreshIntervalForZkReconnectInMs) {
    this.clusterName = clusterName;

    this.refreshAttemptsForZkReconnect = refreshAttemptsForZkReconnect;
    this.refreshIntervalForZkReconnectInMs = refreshIntervalForZkReconnectInMs;

    registerSerializerForSchema(zkClient, helixAdapterSerializer);
    schemaAccessor = new ZkBaseDataAccessor<>(zkClient);
    derivedSchemaAccessor = new ZkBaseDataAccessor<>(zkClient);
    metadataSchemaAccessor = new ZkBaseDataAccessor<>(zkClient);
  }

  private void registerSerializerForSchema(ZkClient zkClient, HelixAdapterSerializer adapter) {
    // Register schema serializer
    String keySchemaPath = getKeySchemaPath(PathResourceRegistry.WILDCARD_MATCH_ANY);
    String valueSchemaPath = getValueSchemaPath(PathResourceRegistry.WILDCARD_MATCH_ANY,
        PathResourceRegistry.WILDCARD_MATCH_ANY);
    String derivedSchemaPath = getDerivedSchemaParentPath(PathResourceRegistry.WILDCARD_MATCH_ANY) + "/" +
        PathResourceRegistry.WILDCARD_MATCH_ANY;
    String metadataSchemaPath = getMetadataSchemaParentPath(PathResourceRegistry.WILDCARD_MATCH_ANY) + "/" +
        PathResourceRegistry.WILDCARD_MATCH_ANY;
    VeniceSerializer<SchemaEntry> serializer = new SchemaEntrySerializer();
    adapter.registerSerializer(keySchemaPath, serializer);
    adapter.registerSerializer(valueSchemaPath, serializer);
    adapter.registerSerializer(derivedSchemaPath, new DerivedSchemaEntrySerializer());
    adapter.registerSerializer(metadataSchemaPath, new MetadataSchemaEntrySerializer());
    zkClient.setZkSerializer(adapter);
  }

  public SchemaEntry getKeySchema(String storeName) {
    return schemaAccessor.get(getKeySchemaPath(storeName), null, AccessOption.PERSISTENT);
  }

  public SchemaEntry getValueSchema(String storeName, String id) {
    return schemaAccessor.get(getValueSchemaPath(storeName, id), null, AccessOption.PERSISTENT);
  }

  public List<SchemaEntry> getAllValueSchemas(String storeName) {
    return HelixUtils.getChildren(schemaAccessor, getValueSchemaParentPath(storeName),
        refreshAttemptsForZkReconnect, refreshIntervalForZkReconnectInMs);
  }

  public DerivedSchemaEntry getDerivedSchema(String storeName, String derivedSchemaIdPair) {
    return derivedSchemaAccessor.get(getDerivedSchemaPath(storeName, derivedSchemaIdPair), null,
        AccessOption.PERSISTENT);
  }

  public List<DerivedSchemaEntry> getAllDerivedSchemas(String storeName) {
    return HelixUtils.getChildren(derivedSchemaAccessor, getDerivedSchemaParentPath(storeName),
        refreshAttemptsForZkReconnect, refreshIntervalForZkReconnectInMs);
  }

  public void createKeySchema(String storeName, SchemaEntry schemaEntry) {
    HelixUtils.create(schemaAccessor, getKeySchemaPath(storeName), schemaEntry);
    logger.info("Set up key schema: " + schemaEntry.toString() + " for store: " + storeName);
  }

  public void addValueSchema(String storeName, SchemaEntry schemaEntry) {
    HelixUtils.create(schemaAccessor, getValueSchemaPath(storeName, String.valueOf(schemaEntry.getId())), schemaEntry);
    logger.info("Added value schema: " + schemaEntry.toString() + " for store: " + storeName);
  }

  public void addDerivedSchema(String storeName, DerivedSchemaEntry derivedSchemaEntry) {
    HelixUtils.create(schemaAccessor, getDerivedSchemaPath(storeName,
        String.valueOf(derivedSchemaEntry.getValueSchemaId()), String.valueOf(derivedSchemaEntry.getId())), derivedSchemaEntry);
    logger.info("Added derived schema: " + derivedSchemaEntry.toString() + "for store: " + storeName);
  }

  public void removeDerivedSchema(String storeName, String derivedSchemaIdPair) {
    HelixUtils.remove(schemaAccessor, getDerivedSchemaPath(storeName, derivedSchemaIdPair));
    logger.info("Removed derived schema for store: " + storeName + " derived schema id pair: " + derivedSchemaIdPair);
  }

  public void subscribeKeySchemaCreationChange(String storeName, IZkChildListener childListener) {
    schemaAccessor.subscribeChildChanges(getKeySchemaParentPath(storeName), childListener);
    logger.info("Subscribe key schema child changes for store: " + storeName);
  }

  public void unsubscribeKeySchemaCreationChange(String storeName, IZkChildListener childListener) {
    schemaAccessor.unsubscribeChildChanges(getKeySchemaParentPath(storeName), childListener);
    logger.info("Unsubscribe key schema child changes for store: " + storeName);
  }

  public void subscribeValueSchemaCreationChange(String storeName, IZkChildListener childListener) {
    schemaAccessor.subscribeChildChanges(getValueSchemaParentPath(storeName), childListener);
    logger.info("Subscribe value schema child changes for store: " + storeName);
  }

  public void unsubscribeValueSchemaCreationChange(String storeName, IZkChildListener childListener) {
    schemaAccessor.unsubscribeChildChanges(getValueSchemaParentPath(storeName), childListener);
    logger.info("Unsubscribe value schema child changes for store: " + storeName);
  }

  public void subscribeDerivedSchemaCreationChange(String storeName, IZkChildListener childListener) {
    derivedSchemaAccessor.subscribeChildChanges(getDerivedSchemaParentPath(storeName), childListener);
    logger.info("Subscribe derived schema child changes for store: " + storeName);
  }

  public void unsubscribeDerivedSchemaCreationChanges(String storeName, IZkChildListener childListener) {
    derivedSchemaAccessor.unsubscribeChildChanges(getDerivedSchemaParentPath(storeName), childListener);
    logger.info("Unsubscribe derived schema child changes for store: " + storeName);
  }

  protected String getStorePath(String storeName) {
    StringBuilder sb = new StringBuilder(HelixUtils.getHelixClusterZkPath(clusterName));
    sb.append(HelixReadOnlyStoreRepository.STORE_REPOSITORY_PATH)
        .append("/")
        .append(VeniceSystemStoreUtils.getZkStoreName(storeName))
        .append("/");
    return sb.toString();
  }

  /**
   * Get absolute key schema parent path for a given store
   */
  String getKeySchemaParentPath(String storeName) {
    return getStorePath(storeName) + KEY_SCHEMA_PATH;
  }

  /**
   * Get absolute key schema path for a given store
   */
  String getKeySchemaPath(String storeName) {
    return getKeySchemaParentPath(storeName) + "/" + KEY_SCHEMA_ID;
  }

  /**
   * Get absolute value schema parent path for a given store
   */
  String getValueSchemaParentPath(String storeName) {
    return getStorePath(storeName) + VALUE_SCHEMA_PATH;
  }

  /**
   * Get absolute value schema path for a given store and schema id
   */
  String getValueSchemaPath(String storeName, String valueSchemaId) {
    return getValueSchemaParentPath(storeName) + "/" + valueSchemaId;
  }

  String getDerivedSchemaParentPath(String storeName) {
    return getStorePath(storeName) + DERIVED_SCHEMA_PATH;
  }

  String getDerivedSchemaPath(String storeName, String valueSchemaId, String derivedSchemaId) {
    return getDerivedSchemaParentPath(storeName) + "/" + valueSchemaId + MULTIPART_SCHEMA_VERSION_DELIMITER + derivedSchemaId;
  }

  String getDerivedSchemaPath(String storeName, String derivedSchemaIdPair) {
    return getDerivedSchemaParentPath(storeName) + "/" + derivedSchemaIdPair;
  }



  public MetadataSchemaEntry getMetadataSchema(String storeName, String timestampMetadataVersionIdPair) {
    return metadataSchemaAccessor.get(getMetadataSchemaPath(storeName, timestampMetadataVersionIdPair), null,
        AccessOption.PERSISTENT);
  }

  public List<MetadataSchemaEntry> getAllMetadataSchemas(String storeName) {
    return HelixUtils.getChildren(metadataSchemaAccessor, getMetadataSchemaParentPath(storeName),
        refreshAttemptsForZkReconnect, refreshIntervalForZkReconnectInMs);
  }

  public void addMetadataSchema(String storeName, MetadataSchemaEntry metadataSchemaEntry) {
    HelixUtils.create(metadataSchemaAccessor, getMetadataSchemaPath(storeName,
        String.valueOf(metadataSchemaEntry.getValueSchemaId()), String.valueOf(metadataSchemaEntry.getId())),
        metadataSchemaEntry);
    logger.info("Added Metadata schema: " + metadataSchemaEntry.toString() + "for store: " + storeName);
  }

  public void subscribeMetadataSchemaCreationChange(String storeName, IZkChildListener childListener) {
    metadataSchemaAccessor.subscribeChildChanges(getMetadataSchemaParentPath(storeName), childListener);
    logger.info("Subscribe Metadata schema child changes for store: " + storeName);
  }

  public void unsubscribeMetadataSchemaCreationChanges(String storeName, IZkChildListener childListener) {
    metadataSchemaAccessor.unsubscribeChildChanges(getMetadataSchemaParentPath(storeName), childListener);
    logger.info("Unsubscribe Metadata schema child changes for store: " + storeName);
  }

  String getMetadataSchemaParentPath(String storeName) {
    return getStorePath(storeName) + METADATA_SCHEMA_PATH;
  }

  String getMetadataSchemaPath(String storeName, String valueSchemaId, String timestampMetadataVersionId) {
    return getMetadataSchemaParentPath(storeName) + "/" + valueSchemaId + MULTIPART_SCHEMA_VERSION_DELIMITER + timestampMetadataVersionId;
  }

  String getMetadataSchemaPath(String storeName, String timestampMetadataVersionIdPair) {
    return getMetadataSchemaParentPath(storeName) + "/" + timestampMetadataVersionIdPair;
  }
}
