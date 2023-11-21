package com.linkedin.venice.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.controllerapi.CurrentVersionResponse;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.QueryAction;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.metadata.response.MetadataResponseRecord;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.service.AbstractVeniceService;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.Time;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * This service is in charge of cleaning up backup versions based on retention policy defined on store basis.
 * If it is not specified, the retention policy will be controlled by config: {@link ConfigKeys#CONTROLLER_BACKUP_VERSION_DEFAULT_RETENTION_MS}.
 * The backup versions will become eligible for removal if the latest current version has been promoted for more
 * than configured retention time period.
 * If the specified retention time is 0, this service won't delete the backup version right after the latest version is
 * promoted to the new current version since there could be a delay before Routers receive the new version promotion notification.
 * Currently, the minimal retention time is hard-coded as 1 hour here: {@link StoreBackupVersionCleanupService#MINIMAL_BACKUP_VERSION_CLEANUP_DELAY}
 * to accommodate the delay between Controller and Router.
 */
public class StoreBackupVersionCleanupService extends AbstractVeniceService {
  public static final String TYPE_CURRENT_VERSION = "current_version";
  private static final Logger LOGGER = LogManager.getLogger(StoreBackupVersionCleanupService.class);
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();

  /**
   * The minimum delay to clean up backup version, and this is used to make sure all the Routers have enough
   * time to switch to the new promoted version.
   */
  private static final long MINIMAL_BACKUP_VERSION_CLEANUP_DELAY = TimeUnit.HOURS.toMillis(1);

  private final VeniceHelixAdmin admin;
  private final VeniceControllerMultiClusterConfig multiClusterConfig;
  private final Set<String> allClusters;
  private final Thread cleanupThread;
  private final long sleepInterval;
  private final long defaultBackupVersionRetentionMs;

  private final Map<String, String> urlMap = new HashMap<>();
  private final AtomicBoolean stop = new AtomicBoolean(false);

  private final CloseableHttpAsyncClient storeClient;

  private final Time time;

  public StoreBackupVersionCleanupService(
      VeniceHelixAdmin admin,
      VeniceControllerMultiClusterConfig multiClusterConfig) {
    this(admin, multiClusterConfig, new SystemTime());
  }

  protected StoreBackupVersionCleanupService(
      VeniceHelixAdmin admin,
      VeniceControllerMultiClusterConfig multiClusterConfig,
      Time time) {
    this.admin = admin;
    this.multiClusterConfig = multiClusterConfig;
    this.allClusters = multiClusterConfig.getClusters();
    this.cleanupThread = new Thread(new StoreBackupVersionCleanupTask(), "StoreBackupVersionCleanupTask");
    this.sleepInterval = TimeUnit.MINUTES.toMillis(5);
    this.defaultBackupVersionRetentionMs = multiClusterConfig.getBackupVersionDefaultRetentionMs();
    this.time = time;
    this.storeClient = HttpAsyncClients.custom()
        .setDefaultRequestConfig(RequestConfig.custom().setSocketTimeout(2000).build())
        .build();
  }

  /**
   * @see AbstractVeniceService#startInner()
   */
  @Override
  public boolean startInner() {
    cleanupThread.start();
    this.storeClient.start();
    return true;
  }

  /**
   * @see AbstractVeniceService#stopInner()
   */
  @Override
  public void stopInner() throws IOException {
    stop.set(true);
    storeClient.close();
    cleanupThread.interrupt();
  }

  protected static boolean whetherStoreReadyToBeCleanup(Store store, long defaultBackupVersionRetentionMs, Time time) {
    long backupVersionRetentionMs = store.getBackupVersionRetentionMs();
    if (backupVersionRetentionMs < 0) {
      backupVersionRetentionMs = defaultBackupVersionRetentionMs;
    }
    if (backupVersionRetentionMs < MINIMAL_BACKUP_VERSION_CLEANUP_DELAY) {
      backupVersionRetentionMs = MINIMAL_BACKUP_VERSION_CLEANUP_DELAY;
    }
    return store.getLatestVersionPromoteToCurrentTimestamp() + backupVersionRetentionMs < time.getMilliseconds();
  }

  private boolean validateAllRouterOnCurrentVersion(Store store, String clusterName, int versionToValidate) {
    List<String> urlList = admin.getHelixVeniceClusterResources(clusterName)
        .getRoutersClusterManager()
        .getLiveRouterInstances()
        .stream()
        .map(
            instance -> urlMap
                .computeIfAbsent(instance, k -> HelixUtils.getInstanceFromHelixInstanceName(k).getUrl(true)))
        .collect(Collectors.toList());

    for (String routerURL: urlList) {
      try {
        HttpGet routerRequest = new HttpGet(routerURL + "/" + TYPE_CURRENT_VERSION + "/" + store.getName());
        HttpResponse response = storeClient.execute(routerRequest, null).get();
        if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
          LOGGER.warn(
              "Got status code {} from host {} while querying current version for store {}",
              response.getStatusLine().getStatusCode(),
              routerURL,
              store.getName());
          return false;
        }
        String responseBody;
        try (InputStream bodyStream = response.getEntity().getContent()) {
          responseBody = IOUtils.toString(bodyStream);
        }
        CurrentVersionResponse currentVersionResponse =
            OBJECT_MAPPER.readValue(responseBody.getBytes(), CurrentVersionResponse.class);
        if (currentVersionResponse.getCurrentVersion() != versionToValidate) {
          return false;
        }
      } catch (Exception e) {
        LOGGER.error("Got exception while getting current version for store {}", store.getName(), e);
        return false;
      }
    }
    return true;
  }

  private boolean validateAllServerOnCurrentVersion(Store store, String clusterName, int versionToValidate) {
    Set<Instance> instances = admin.getLiveInstanceMonitor(clusterName).getAllLiveInstances();

    for (Instance instance: instances) {
      try {
        HttpGet routerRequest = new HttpGet(
            instance.getUrl() + "/" + QueryAction.METADATA.toString().toLowerCase() + "/" + store.getName());
        HttpResponse response = storeClient.execute(routerRequest, null).get();
        if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
          LOGGER.warn(
              "Got status code {} from host {} while querying current version for store {}",
              response.getStatusLine().getStatusCode(),
              instance,
              store.getName());
          return false;
        }
        byte[] responseBody;
        try (InputStream bodyStream = response.getEntity().getContent()) {
          responseBody = IOUtils.toByteArray(bodyStream);
        }
        RecordDeserializer<MetadataResponseRecord> metadataResponseRecordRecordDeserializer =
            SerializerDeserializerFactory.getAvroGenericDeserializer(MetadataResponseRecord.SCHEMA$);
        GenericRecord metadataResponse = metadataResponseRecordRecordDeserializer.deserialize(responseBody);

        MetadataResponseRecord metadataResponseRecord =
            OBJECT_MAPPER.readValue(metadataResponse.toString(), MetadataResponseRecord.class);
        if (metadataResponseRecord.getVersionMetadata().getCurrentVersion() != versionToValidate) {
          return false;
        }
      } catch (Exception e) {
        LOGGER.error("Got exception while getting server current version for store {}", store.getName(), e);
        return false;
      }
    }

    return true;
  }

  /**
   * Using a separate function for store cleanup is to make it easy for testing.
   * @return whether any backup version is removed or not
   */
  protected boolean cleanupBackupVersion(Store store, String clusterName) {
    if (!whetherStoreReadyToBeCleanup(store, defaultBackupVersionRetentionMs, time)) {
      // not ready to clean up backup versions yet
      return false;
    }

    List<Version> versions = store.getVersions();
    List<Version> readyToBeRemovedVersions = new ArrayList<>();
    int currentVersion = store.getCurrentVersion();

    // Do not delete version unless all routers and all servers are on same current version
    if (!validateAllRouterOnCurrentVersion(store, clusterName, currentVersion)
        || !validateAllServerOnCurrentVersion(store, clusterName, currentVersion)) {
      return false;
    }

    versions.forEach(v -> {
      if (v.getNumber() < currentVersion) {
        readyToBeRemovedVersions.add(v);
      }
    });
    if (readyToBeRemovedVersions.isEmpty()) {
      return false;
    }
    String storeName = store.getName();
    LOGGER.info(
        "Started removing backup versions according to retention policy for store: {} in cluster: {}",
        storeName,
        clusterName);
    readyToBeRemovedVersions.forEach(v -> {
      int versionNum = v.getNumber();
      LOGGER.info(
          "Version: {} of store: {} in cluster: {} will be removed according to backup version retention policy",
          versionNum,
          storeName,
          clusterName);
      try {
        // deleteOldVersionInStore will run additional check to avoid deleting current version
        admin.deleteOldVersionInStore(clusterName, storeName, versionNum);
      } catch (Exception e) {
        LOGGER.error(
            "Encountered exception while trying to delete version: {}, store: {}, in cluster: {}",
            versionNum,
            storeName,
            clusterName,
            e);
      }
    });
    LOGGER.info(
        "Finished removing backup versions according to retention policy for store: {} in cluster: {}",
        storeName,
        clusterName);
    return true;
  }

  private class StoreBackupVersionCleanupTask implements Runnable {
    @Override
    public void run() {
      boolean interruptReceived = false;
      while (!stop.get()) {
        try {
          time.sleep(sleepInterval);
        } catch (InterruptedException e) {
          LOGGER.error("Received InterruptedException during sleep in StoreBackupVersionCleanupTask thread");
          break;
        }
        // loop all the clusters
        for (String clusterName: allClusters) {
          boolean cleanupEnabled =
              multiClusterConfig.getControllerConfig(clusterName).isBackupVersionRetentionBasedCleanupEnabled();
          if (!cleanupEnabled || !admin.isLeaderControllerFor(clusterName)) {
            // Only do backup version retention with cluster level config enabled in leader controller for current
            // cluster
            continue;
          }
          // Get all stores for current cluster
          List<Store> stores = admin.getAllStores(clusterName);
          for (Store store: stores) {
            boolean didCleanup = false;
            try {
              didCleanup = cleanupBackupVersion(store, clusterName);
            } catch (Exception e) {
              LOGGER.error(
                  "Encountered exception while handling backup version cleanup for store: {} in cluster: {}",
                  store.getName(),
                  clusterName,
                  e);
            }
            if (didCleanup) {
              try {
                time.sleep(sleepInterval);
              } catch (InterruptedException e) {
                interruptReceived = true;
                LOGGER.error("Received InterruptedException during sleep in StoreBackupVersionCleanupTask thread");
                break;
              }
            }
          }
          if (interruptReceived) {
            break;
          }
        }
      }
      LOGGER.info("StoreBackupVersionCleanupTask stopped.");
    }
  }
}
