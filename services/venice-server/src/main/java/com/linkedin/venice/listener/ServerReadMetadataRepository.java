package com.linkedin.venice.listener;

import com.linkedin.davinci.listener.response.MetadataResponse;
import com.linkedin.davinci.listener.response.MetadataWithStorePropertiesResponse;
import com.linkedin.davinci.listener.response.ServerCurrentVersionResponse;
import com.linkedin.davinci.stats.ServerMetadataServiceStats;
import com.linkedin.davinci.storage.ReadMetadataRetriever;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.HelixCustomizedViewOfflinePushRepository;
import com.linkedin.venice.helix.HelixInstanceConfigRepository;
import com.linkedin.venice.helix.HelixReadOnlyStoreConfigRepository;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStore;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreConfig;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.metadata.response.VersionProperties;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.systemstore.schemas.StoreKeySchemas;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.systemstore.schemas.StoreProperties;
import com.linkedin.venice.systemstore.schemas.StoreValueSchemas;
import com.linkedin.venice.utils.HelixUtils;
import io.tehuti.metrics.MetricsRepository;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * A wrapper that holds reference for various repositories responsible for constructing metadata responses upon request.
 */
public class ServerReadMetadataRepository implements ReadMetadataRetriever {
  private static final Logger LOGGER = LogManager.getLogger(ServerReadMetadataRepository.class);
  private final String serverCluster;
  private final ServerMetadataServiceStats serverMetadataServiceStats;
  private final ReadOnlyStoreRepository storeRepository;
  private final ReadOnlySchemaRepository schemaRepository;
  private HelixCustomizedViewOfflinePushRepository customizedViewRepository;
  private HelixInstanceConfigRepository helixInstanceConfigRepository;
  private HelixReadOnlyStoreConfigRepository storeConfigRepository;

  public ServerReadMetadataRepository(
      String serverCluster,
      MetricsRepository metricsRepository,
      ReadOnlyStoreRepository storeRepository,
      ReadOnlySchemaRepository schemaRepository,
      HelixReadOnlyStoreConfigRepository storeConfigRepository,
      Optional<CompletableFuture<HelixCustomizedViewOfflinePushRepository>> customizedViewFuture,
      Optional<CompletableFuture<HelixInstanceConfigRepository>> helixInstanceFuture) {
    this.serverCluster = serverCluster;
    this.serverMetadataServiceStats = new ServerMetadataServiceStats(metricsRepository);
    this.storeRepository = storeRepository;
    this.schemaRepository = schemaRepository;
    this.storeConfigRepository = storeConfigRepository;

    customizedViewFuture.ifPresent(future -> future.thenApply(cv -> this.customizedViewRepository = cv));
    helixInstanceFuture.ifPresent(future -> future.thenApply(helix -> this.helixInstanceConfigRepository = helix));
  }

  /**
   * Return the metadata information for the given store. The data is retrieved from its respective repositories which
   * originate from the VeniceServer.
   * @param storeName
   * @return {@link MetadataResponse} object that holds all the information required for answering a server metadata
   * fetch request.
   */
  @Override
  public MetadataResponse getMetadata(String storeName) {
    serverMetadataServiceStats.recordRequestBasedMetadataInvokeCount();
    MetadataResponse response = new MetadataResponse();
    try {
      Store store = storeRepository.getStoreOrThrow(storeName);
      checkStore(storeName, store);

      // Version metadata
      int currentVersionNumber = getCurrentVersionNumberOrThrow(storeName, store);
      Version currentVersion = store.getVersionOrThrow(currentVersionNumber);
      Map<CharSequence, CharSequence> partitionerParams =
          new HashMap<>(currentVersion.getPartitionerConfig().getPartitionerParams());
      VersionProperties versionProperties = new VersionProperties(
          currentVersionNumber,
          currentVersion.getCompressionStrategy().getValue(),
          currentVersion.getPartitionCount(),
          currentVersion.getPartitionerConfig().getPartitionerClass(),
          partitionerParams,
          currentVersion.getPartitionerConfig().getAmplificationFactor());

      // Versions
      List<Integer> versions = new ArrayList<>();
      for (Version v: store.getVersions()) {
        versions.add(v.getNumber());
      }

      // Schema metadata
      Map<CharSequence, CharSequence> keySchema = Collections.singletonMap(
          String.valueOf(schemaRepository.getKeySchema(storeName).getId()),
          schemaRepository.getKeySchema(storeName).getSchema().toString());
      Map<CharSequence, CharSequence> valueSchemas = new HashMap<>();
      int latestSuperSetValueSchemaId = store.getLatestSuperSetValueSchemaId();
      for (SchemaEntry schemaEntry: schemaRepository.getValueSchemas(storeName)) {
        valueSchemas.put(String.valueOf(schemaEntry.getId()), schemaEntry.getSchema().toString());
      }

      // Routing metadata
      Map<CharSequence, List<CharSequence>> routingInfo = getRoutingInfo(storeName, currentVersionNumber);

      // Helix metadata
      Map<CharSequence, Integer> helixGroupInfo = new HashMap<>();
      for (Map.Entry<String, Integer> entry: helixInstanceConfigRepository.getInstanceGroupIdMapping().entrySet()) {
        helixGroupInfo.put(HelixUtils.instanceIdToUrl(entry.getKey()), entry.getValue());
      }

      response.setVersionMetadata(versionProperties);
      response.setVersions(versions);
      response.setKeySchema(keySchema);
      response.setValueSchemas(valueSchemas);
      response.setLatestSuperSetValueSchemaId(latestSuperSetValueSchemaId);
      response.setRoutingInfo(routingInfo);
      response.setHelixGroupInfo(helixGroupInfo);
      if (store.getBatchGetLimit() > 0) {
        response.setBatchGetLimit(store.getBatchGetLimit());
      } else {
        response.setBatchGetLimit(Store.DEFAULT_BATCH_GET_LIMIT);
      }
    } catch (VeniceException e) {
      LOGGER.warn("Failed to populate request based metadata for store: {}.", storeName);
      response.setMessage("Failed to populate metadata for store: " + storeName + " due to: " + e.getMessage());
      response.setError(true);
      serverMetadataServiceStats.recordRequestBasedMetadataFailureCount();
    }
    return response;
  }

  /**
   * Return the metadata information for the given store. The data is retrieved from its respective repositories which
   * originate from the VeniceServer.
   * @param storeName
   * @return {@link MetadataResponse} object that holds all the information required for answering a server metadata
   * fetch request.
   */
  @Override
  public MetadataWithStorePropertiesResponse getMetadataWithStoreProperties(String storeName) {
    serverMetadataServiceStats.recordRequestBasedMetadataInvokeCount();
    MetadataWithStorePropertiesResponse response = new MetadataWithStorePropertiesResponse();

    try {
      Store store = storeRepository.getStoreOrThrow(storeName);
      checkStore(storeName, store);

      // Store Properties
      StoreProperties storeProperties = ((ReadOnlyStore) store).getStoreProperties();

      // Key Schemas
      Map<CharSequence, CharSequence> keySchema = Collections.singletonMap(
          String.valueOf(schemaRepository.getKeySchema(storeName).getId()),
          schemaRepository.getKeySchema(storeName).getSchema().toString());
      StoreKeySchemas storeKeySchemas = new StoreKeySchemas();
      storeKeySchemas.setKeySchemaMap(keySchema);

      // Value Schemas
      Map<CharSequence, CharSequence> valueSchemas = new HashMap<>();
      for (SchemaEntry schemaEntry: schemaRepository.getValueSchemas(storeName)) {
        valueSchemas.put(String.valueOf(schemaEntry.getId()), schemaEntry.getSchema().toString());
      }
      StoreValueSchemas storeValueSchemas = new StoreValueSchemas();
      storeValueSchemas.setValueSchemaMap(valueSchemas);

      // IdsWrittenPerStoreVersion
      int latestSuperSetValueSchemaId = store.getLatestSuperSetValueSchemaId();
      ArrayList<Integer> idsWrittenPerStoreVersion = new ArrayList<>();
      idsWrittenPerStoreVersion.add(latestSuperSetValueSchemaId);

      // Routing metadata
      int currentVersionNumber = getCurrentVersionNumberOrThrow(storeName, store);
      Map<CharSequence, List<CharSequence>> routingInfo = getRoutingInfo(storeName, currentVersionNumber);

      // StoreMetaValue
      StoreMetaValue storeMetaValue = new StoreMetaValue();
      storeMetaValue.setTimestamp(System.currentTimeMillis());
      storeMetaValue.setStoreProperties(storeProperties);
      storeMetaValue.setStoreKeySchemas(storeKeySchemas);
      storeMetaValue.setStoreValueSchemas(storeValueSchemas);
      storeMetaValue.setStoreValueSchemaIdsWrittenPerStoreVersion(idsWrittenPerStoreVersion);

      // Helix metadata
      Map<CharSequence, Integer> helixGroupInfo = new HashMap<>();
      for (Map.Entry<String, Integer> entry: helixInstanceConfigRepository.getInstanceGroupIdMapping().entrySet()) {
        helixGroupInfo.put(HelixUtils.instanceIdToUrl(entry.getKey()), entry.getValue());
      }

      response.setStoreMetaValue(storeMetaValue);
      response.setHelixGroupInfo(helixGroupInfo);
      response.setRoutingInfo(routingInfo);
    } catch (VeniceException e) {
      LOGGER.warn("Failed to populate request based metadata by client for store: {}.", storeName);
      response
          .setMessage("Failed to populate metadata by client for store: " + storeName + " due to: " + e.getMessage());
      response.setError(true);
      serverMetadataServiceStats.recordRequestBasedMetadataFailureCount();
    } catch (Exception e) {
      StringWriter sw = new StringWriter();
      try (PrintWriter pw = new PrintWriter(sw)) {
        e.printStackTrace(pw);
      }
      String trace = sw.toString();
      LOGGER.warn("Failed to populate request based metadata by client for store: {}.", storeName);
      response.setMessage(
          "Failed to populate metadata by client for store: " + storeName + " due to: " + e.toString() + "\n" + trace);
      response.setError(true);
      serverMetadataServiceStats.recordRequestBasedMetadataFailureCount();
    }

    return response;
  }

  @Override
  public ServerCurrentVersionResponse getCurrentVersionResponse(String storeName) {
    ServerCurrentVersionResponse response = new ServerCurrentVersionResponse();
    try {
      Store store = storeRepository.getStoreOrThrow(storeName);
      // Version metadata
      int currentVersionNumber = store.getCurrentVersion();
      if (currentVersionNumber == Store.NON_EXISTING_VERSION) {
        throw new VeniceException(
            "No valid store version available to read for store: " + storeName
                + ". Please push data to the store before consuming");
      }
      response.setCurrentVersion(currentVersionNumber);
    } catch (VeniceException e) {
      response.setMessage("Failed to get current version for store: " + storeName + " due to: " + e.getMessage());
      response.setError(true);
    }
    return response;
  }

  private void checkStore(String storeName, Store store) throws VeniceException, UnsupportedOperationException {

    // Check fast client compatibility
    if (!store.isStorageNodeReadQuotaEnabled()) {
      throw new UnsupportedOperationException(
          String.format(
              "Fast client is not enabled for store: %s, please ensure storage node read quota is enabled for the given store",
              storeName));
    }

    if (store.isMigrating()) {
      // only obtain store Config when store is migrating and only throw exceptions when dest cluster is ready or
      // store config is not available
      StoreConfig storeConfig = storeConfigRepository.getStoreConfigOrThrow(storeName);
      String storeCluster = storeConfig.getCluster();
      if (storeCluster == null) {
        // defensive check
        throw new VeniceException("Store: " + storeName + " is migrating but store cluster is not set.");
      }
      // store cluster has changed so throw exception to enforce client to do a new service discovery
      if (!storeCluster.equals(serverCluster)) {
        throw new VeniceException(
            "Store: " + storeName + " is migrating. Failing the request to allow fast "
                + "client refresh service discovery.");
      }
    }
  }

  private int getCurrentVersionNumberOrThrow(String storeName, Store store) throws VeniceException {

    // Version metadata
    int currentVersionNumber = store.getCurrentVersion();
    if (currentVersionNumber == Store.NON_EXISTING_VERSION) {
      throw new VeniceException(
          "No valid store version available to read for store: " + storeName
              + ". Please push data to the store before consuming");
    }

    return currentVersionNumber;
  }

  private Map<CharSequence, List<CharSequence>> getRoutingInfo(String storeName, int currentVersionNumber) {

    // Routing metadata
    Map<CharSequence, List<CharSequence>> routingInfo = new HashMap<>();
    String currentVersionResource = Version.composeKafkaTopic(storeName, currentVersionNumber);
    for (Partition partition: customizedViewRepository.getPartitionAssignments(currentVersionResource)
        .getAllPartitions()) {
      List<CharSequence> instances = new ArrayList<>();
      for (Instance instance: partition.getReadyToServeInstances()) {
        instances.add(instance.getUrl(true));
      }
      routingInfo.put(String.valueOf(partition.getId()), instances);
    }

    return routingInfo;
  }
}
