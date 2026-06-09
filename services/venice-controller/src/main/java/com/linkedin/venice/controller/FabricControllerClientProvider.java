package com.linkedin.venice.controller;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.D2ControllerClient;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.io.Closeable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang.StringUtils;


/**
 * Owns the controller-client maps used for cross-region / cross-fabric coordination and centralizes their lazy
 * construction and lifecycle. A single instance is created by {@link VeniceHelixAdmin} (child) and shared with
 * {@link VeniceParentHelixAdmin} (parent), which previously each maintained their own copy of this plumbing.
 *
 * Two maps are maintained:
 * <ul>
 *   <li>{@link #clusterControllerClientPerColoMap}: cluster -&gt; colo -&gt; client, built from the standard child
 *       data-center allowlist (URL and D2 maps) in the cluster config.</li>
 *   <li>{@link #newFabricControllerClientMap}: cluster -&gt; fabric -&gt; client, built on demand for fabrics that are
 *       not in the standard allowlist (e.g. build-out / data-recovery destinations).</li>
 * </ul>
 *
 * Both maps are {@link VeniceConcurrentHashMap}s populated via {@code computeIfAbsent}; no external locking is needed.
 */
public class FabricControllerClientProvider implements Closeable {
  private final VeniceControllerMultiClusterConfig multiClusterConfigs;
  private final Optional<SSLFactory> sslFactory;
  private final Map<String, D2Client> d2Clients;

  /** Controller Client Map per cluster per colo */
  private final Map<String, Map<String, ControllerClient>> clusterControllerClientPerColoMap =
      new VeniceConcurrentHashMap<>();

  /** New fabric controller client map per cluster per fabric */
  private final Map<String, Map<String, ControllerClient>> newFabricControllerClientMap =
      new VeniceConcurrentHashMap<>();

  public FabricControllerClientProvider(
      VeniceControllerMultiClusterConfig multiClusterConfigs,
      Optional<SSLFactory> sslFactory,
      Map<String, D2Client> d2Clients) {
    this.multiClusterConfigs = multiClusterConfigs;
    this.sslFactory = sslFactory;
    this.d2Clients = d2Clients;
  }

  /**
   * Returns the lazily-constructed map of controller clients (colo -&gt; client) for the standard child data-center
   * allowlist of the given cluster.
   */
  public Map<String, ControllerClient> getControllerClientMap(String clusterName) {
    return clusterControllerClientPerColoMap.computeIfAbsent(clusterName, cn -> {
      Map<String, ControllerClient> controllerClients = new HashMap<>();
      VeniceControllerClusterConfig controllerConfig = multiClusterConfigs.getControllerConfig(clusterName);
      controllerConfig.getChildDataCenterControllerUrlMap()
          .entrySet()
          .forEach(
              entry -> controllerClients.put(
                  entry.getKey(),
                  ControllerClient.constructClusterControllerClient(clusterName, entry.getValue(), sslFactory)));

      // Respect d2Clients from controller constructor, if not provided, create d2 clients by zk d2 service urls
      // (mainly for testing purpose)
      if (d2Clients != null) {
        controllerConfig.getChildDataCenterControllerD2Map()
            .entrySet()
            .forEach(
                entry -> controllerClients.put(
                    entry.getKey(),
                    new D2ControllerClient(
                        controllerConfig.getD2ServiceName(),
                        clusterName,
                        d2Clients.get(entry.getKey()),
                        sslFactory)));
      } else {
        controllerConfig.getChildDataCenterControllerD2Map()
            .entrySet()
            .forEach(
                entry -> controllerClients.put(
                    entry.getKey(),
                    new D2ControllerClient(
                        controllerConfig.getD2ServiceName(),
                        clusterName,
                        entry.getValue(),
                        sslFactory)));
      }

      return controllerClients;
    });
  }

  /**
   * Returns a controller client for a specific fabric of the given cluster. Fabrics in the standard allowlist are
   * served from {@link #getControllerClientMap(String)}; fabrics outside the allowlist (e.g. build-out / data-recovery
   * destinations) are built on demand from child cluster configs and cached in {@link #newFabricControllerClientMap}.
   */
  public ControllerClient getFabricBuildoutControllerClient(String clusterName, String fabric) {
    Map<String, ControllerClient> controllerClients = getControllerClientMap(clusterName);
    if (controllerClients.containsKey(fabric)) {
      return controllerClients.get(fabric);
    }

    // For fabrics not in allowlist, build controller clients using child cluster configs and cache them in another map
    ControllerClient value =
        newFabricControllerClientMap.computeIfAbsent(clusterName, cn -> new VeniceConcurrentHashMap<>())
            .computeIfAbsent(fabric, f -> {
              VeniceControllerClusterConfig controllerConfig = multiClusterConfigs.getControllerConfig(clusterName);
              String d2ZkHost = controllerConfig.getChildControllerD2ZkHost(fabric);
              String d2ServiceName = controllerConfig.getD2ServiceName();
              if (StringUtils.isNotBlank(d2ZkHost) && StringUtils.isNotBlank(d2ServiceName)) {
                if (d2Clients != null) {
                  return new D2ControllerClient(d2ServiceName, clusterName, d2Clients.get(fabric));
                }
                return new D2ControllerClient(d2ServiceName, clusterName, d2ZkHost, sslFactory);
              }
              String url = controllerConfig.getChildControllerUrl(fabric);
              if (StringUtils.isNotBlank(url)) {
                return ControllerClient.constructClusterControllerClient(clusterName, url, sslFactory);
              }
              return null;
            });

    if (value == null) {
      throw new VeniceException(
          "Could not construct child controller client for cluster " + clusterName + " fabric " + fabric
              + ". child.cluster.d2 or child.cluster.url value is missing in parent controller");
    }
    return value;
  }

  @Override
  public void close() {
    clusterControllerClientPerColoMap.values()
        .forEach(controllerClientMap -> controllerClientMap.values().forEach(Utils::closeQuietlyWithErrorLogged));
    newFabricControllerClientMap.values()
        .forEach(controllerClientMap -> controllerClientMap.values().forEach(Utils::closeQuietlyWithErrorLogged));
  }
}
