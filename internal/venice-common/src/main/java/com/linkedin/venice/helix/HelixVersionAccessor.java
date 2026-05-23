package com.linkedin.venice.helix;

import static com.linkedin.venice.zk.VeniceZkPaths.STORES;

import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.PathResourceRegistry;
import java.util.Collections;
import java.util.List;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Accessor for individual version znodes that live as children of a store znode:
 * {@code /<cluster>/Stores/<storeName>/versions/<versionNumber>}.
 *
 * The accessor is the controller-side write path and the reader-side read path for per-version data. The legacy
 * "embedded versions list inside the store JSON" representation is handled by the store repository; this accessor
 * deals only with the new layout.
 */
public class HelixVersionAccessor {
  private static final Logger LOGGER = LogManager.getLogger(HelixVersionAccessor.class);

  static final String VERSIONS_SUB_PATH = "versions";

  private static final int DEFAULT_ZK_REFRESH_ATTEMPTS = 9;

  private final String clusterStoresPath;
  private final ZkBaseDataAccessor<Version> versionAccessor;
  private final int refreshAttemptsForZkReconnect;

  public HelixVersionAccessor(ZkClient zkClient, HelixAdapterSerializer adapterSerializer, String clusterName) {
    this(zkClient, adapterSerializer, clusterName, DEFAULT_ZK_REFRESH_ATTEMPTS);
  }

  public HelixVersionAccessor(
      ZkClient zkClient,
      HelixAdapterSerializer adapterSerializer,
      String clusterName,
      int refreshAttemptsForZkReconnect) {
    this.clusterStoresPath = HelixUtils.getHelixClusterZkPath(clusterName) + "/" + STORES;
    this.refreshAttemptsForZkReconnect = refreshAttemptsForZkReconnect;
    registerSerializer(zkClient, adapterSerializer);
    this.versionAccessor = new ZkBaseDataAccessor<>(zkClient);
  }

  private void registerSerializer(ZkClient zkClient, HelixAdapterSerializer adapterSerializer) {
    String versionWildcardPath = getVersionZkPath(PathResourceRegistry.WILDCARD_MATCH_ANY, "*");
    adapterSerializer.registerSerializer(versionWildcardPath, new VersionJSONSerializer());
    zkClient.setZkSerializer(adapterSerializer);
  }

  public String getVersionsContainerPath(String storeName) {
    return clusterStoresPath + "/" + storeName + "/" + VERSIONS_SUB_PATH;
  }

  public String getVersionZkPath(String storeName, int versionNumber) {
    return getVersionZkPath(storeName, String.valueOf(versionNumber));
  }

  private String getVersionZkPath(String storeName, String versionNumberToken) {
    return getVersionsContainerPath(storeName) + "/" + versionNumberToken;
  }

  public boolean hasVersionsContainer(String storeName) {
    return HelixUtils.exists(versionAccessor, getVersionsContainerPath(storeName));
  }

  public Version getVersion(String storeName, int versionNumber) {
    return versionAccessor.get(getVersionZkPath(storeName, versionNumber), null, AccessOption.PERSISTENT);
  }

  public List<Version> getVersionsForStore(String storeName) {
    String containerPath = getVersionsContainerPath(storeName);
    if (!HelixUtils.exists(versionAccessor, containerPath)) {
      return Collections.emptyList();
    }
    return HelixUtils.getChildren(versionAccessor, containerPath, refreshAttemptsForZkReconnect);
  }

  public List<String> getVersionNumbersForStore(String storeName) {
    String containerPath = getVersionsContainerPath(storeName);
    if (!HelixUtils.exists(versionAccessor, containerPath)) {
      return Collections.emptyList();
    }
    return HelixUtils.listPathContents(versionAccessor, containerPath);
  }

  public void putVersion(String storeName, Version version) {
    String path = getVersionZkPath(storeName, version.getNumber());
    if (HelixUtils.exists(versionAccessor, path)) {
      HelixUtils.update(versionAccessor, path, version);
    } else {
      HelixUtils.create(versionAccessor, path, version);
    }
  }

  public void removeVersion(String storeName, int versionNumber) {
    String path = getVersionZkPath(storeName, versionNumber);
    if (HelixUtils.exists(versionAccessor, path)) {
      HelixUtils.remove(versionAccessor, path);
    }
  }

  public void removeAllVersionsForStore(String storeName) {
    String containerPath = getVersionsContainerPath(storeName);
    if (!HelixUtils.exists(versionAccessor, containerPath)) {
      return;
    }
    for (String versionNumberToken: HelixUtils.listPathContents(versionAccessor, containerPath)) {
      HelixUtils.remove(versionAccessor, getVersionZkPath(storeName, versionNumberToken));
    }
    HelixUtils.remove(versionAccessor, containerPath);
    LOGGER.info("Removed all per-version znodes under {}", containerPath);
  }
}
