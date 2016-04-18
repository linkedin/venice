package com.linkedin.venice.integration.utils;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.controller.VeniceControllerClusterConfig;
import com.linkedin.venice.controller.VeniceControllerConfig;
import com.linkedin.venice.controller.VeniceControllerService;
import com.linkedin.venice.utils.Props;

import java.io.File;

/**
 * A wrapper for the {@link VeniceControllerWrapper}.
 *
 * Calling close() will clean up the controller's data directory.
 */
public class VeniceControllerWrapper extends ProcessWrapper {

  public static final String SERVICE_NAME = "VeniceController";

  private final VeniceControllerService service;
  private final String clusterName;
  private final int adminPort;

  VeniceControllerWrapper(
      String serviceName,
      File dataDirectory,
      VeniceControllerService service,
      String clusterName,
      int adminPort) {
    super(serviceName, dataDirectory);
    this.service = service;
    this.clusterName = clusterName;
    this.adminPort = adminPort;
  }

  static StatefulServiceProvider<VeniceControllerWrapper> generateService(String clusterName, KafkaBrokerWrapper kafkaBrokerWrapper) {
    // TODO: Once the ZK address used by Controller and Kafka are decoupled, change this
    String zkAddress = kafkaBrokerWrapper.getZkAddress();

    return (serviceName, port, dataDirectory) -> {
      Props clusterProps = TestUtils.getClusterProps(clusterName, dataDirectory, kafkaBrokerWrapper);

      // TODO: Validate that these configs are all still used.
      // TODO: Centralize default config values in a single place

      clusterProps.put(VeniceControllerClusterConfig.KAFKA_REPLICA_FACTOR, 1);
      clusterProps.put(VeniceControllerClusterConfig.KAFKA_ZK_ADDRESS, zkAddress);
      clusterProps.put(VeniceControllerClusterConfig.CONTROLLER_NAME, "venice-controller"); // Why is this configurable?
      clusterProps.put(VeniceControllerClusterConfig.REPLICA_FACTOR, 1);
      clusterProps.put(VeniceControllerClusterConfig.NUMBER_OF_PARTITION, 1);

      Props controllerProps = new Props();

      int adminPort = TestUtils.getFreePort();
      controllerProps.put(ConfigKeys.ADMIN_PORT, adminPort);

      Props props = clusterProps.mergeWithProperties(controllerProps);
      VeniceControllerConfig config = new VeniceControllerConfig(props);
      VeniceControllerService veniceController = new VeniceControllerService(config);
      return new VeniceControllerWrapper(serviceName, dataDirectory, veniceController, clusterName, adminPort);
    };
  }

  @Override
  public String getHost() {
    return DEFAULT_HOST_NAME;
  }

  @Override
  public int getPort() {
    return adminPort;
  }

  public String getControllerUrl() {
    return "http://" + getHost() + ":" + getPort();
  }

  @Override
  protected void start() throws Exception {
    service.start();
  }

  @Override
  protected void stop() throws Exception {
    service.stop();
  }

  /**
   * Creates a new store and initializes version 1 of that store.
   *
   * @return the name of the newly-created store-version
   */
  public String getNewStoreVersion() {
    String storeName = TestUtils.getUniqueString("venice-store");
    String storeOwner = TestUtils.getUniqueString("store-owner");
    this.service.getVeniceHelixAdmin().addStore(clusterName, storeName, storeOwner);
    this.service.getVeniceHelixAdmin().addVersion(clusterName, storeName, 1);
    this.service.getVeniceHelixAdmin().setCurrentVersion(clusterName, storeName, 1);

    return storeName + "_v1"; // TODO: Decide if it is acceptable to hard-code this here...
  }
}
