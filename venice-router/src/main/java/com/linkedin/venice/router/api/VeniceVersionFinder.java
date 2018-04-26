package com.linkedin.venice.router.api;

import com.linkedin.ddsstorage.router.api.RouterException;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.RoutingDataRepository;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.HelixUtils;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.validation.constraints.NotNull;
import org.apache.http.client.methods.HttpGet;
import org.apache.log4j.Logger;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

public class VeniceVersionFinder {
  private static final Logger logger = Logger.getLogger(VeniceVersionFinder.class);

  private final ReadOnlyStoreRepository metadataRepository;
  private final Optional<RoutingDataRepository> routingData;
  private ConcurrentMap<String, Integer> lastCurrentVersion = new ConcurrentHashMap<>();

  /**
   *
   * @param metadataRepository for getting the current version from zookeeper
   * @param routingData for validating that a new version has online replicas.  Pass null to disable this check (for tests)
   */
  public VeniceVersionFinder(@NotNull ReadOnlyStoreRepository metadataRepository, Optional<RoutingDataRepository> routingData){
    this.metadataRepository = metadataRepository;
    this.routingData = routingData;
  }

  public int getVersion(@NotNull String store)
      throws RouterException {
    /**
     * TODO: clone a store object is too expensive, and we could choose to expose the necessary methods
     * in {@link ReadOnlyStoreRepository}, such as 'isEnableReads' and others.
     */
    Store veniceStore = metadataRepository.getStore(store);
    if (null == veniceStore){
      throw new RouterException(HttpResponseStatus.class, HttpResponseStatus.BAD_REQUEST, HttpResponseStatus.BAD_REQUEST.getCode(),
          "Store: " + store + " does not exist on this cluster", false);
    }
    if (!veniceStore.isEnableReads()) {
      throw new RouterException(HttpResponseStatus.class, HttpResponseStatus.FORBIDDEN,
          HttpResponseStatus.FORBIDDEN.getCode(),
          "Could not read from store: " + store + ", because it's disabled from reading.", false);
    }

    int metadataCurrentVersion = veniceStore.getCurrentVersion();
    if (! lastCurrentVersion.containsKey(store)){
      lastCurrentVersion.put(store, metadataCurrentVersion);
    }
    if (lastCurrentVersion.get(store).equals(metadataCurrentVersion)){
      return metadataCurrentVersion;
    }
   //This is a new version change, verify we have online replicas for each partition
    String kafkaTopic = Version.composeKafkaTopic(store, metadataCurrentVersion);
    int partitionCount = veniceStore.getPartitionCount();
    if (anyOfflinePartitions(routingData, kafkaTopic, partitionCount)){
      logger.warn("Offline partitions for new active version " + kafkaTopic
          + ", continuing to serve previous version: " + lastCurrentVersion.get(store));
      return lastCurrentVersion.get(store);
    } else { // all partitions are online
      lastCurrentVersion.put(store, metadataCurrentVersion);
      return metadataCurrentVersion;
    }
  }

  private static boolean anyOfflinePartitions(Optional<RoutingDataRepository> routingData, String kafkaTopic, int partitionCount) {
    if (routingData.isPresent()) { //If we passed an empty routingData object, assume the new version is safe
      for (int p = 0; p < partitionCount; p++) {
        List<Instance> partitionHosts = routingData.get().getReadyToServeInstances(kafkaTopic, p);
        if (partitionHosts.isEmpty()) {
          return true;
        }
      }
    }
    return false;
  }
}
