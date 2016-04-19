package com.linkedin.venice.integration.utils;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.controller.VeniceController;
import com.linkedin.venice.controller.VeniceControllerClusterConfig;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreCreationResponse;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.VeniceProperties;

import java.io.File;

/**
 * A wrapper for the {@link VeniceControllerWrapper}.
 *
 * Calling close() will clean up the controller's data directory.
 */
public class VeniceControllerWrapper extends ProcessWrapper {

  public static final String SERVICE_NAME = "VeniceController";
  private final VeniceController service;
  private final int port;

  VeniceControllerWrapper(
      String serviceName,
      File dataDirectory,
      VeniceController service,
      int port) {
    super(serviceName, dataDirectory);
    this.service = service;
    this.port = port;
  }

  static StatefulServiceProvider<VeniceControllerWrapper> generateService(String clusterName, KafkaBrokerWrapper kafkaBrokerWrapper) {
    // TODO: Once the ZK address used by Controller and Kafka are decoupled, change this
    String zkAddress = kafkaBrokerWrapper.getZkAddress();

    return (serviceName, port, dataDirectory) -> {
      VeniceProperties clusterProps = TestUtils.getClusterProps(clusterName, dataDirectory, kafkaBrokerWrapper);

      int adminPort = TestUtils.getFreePort();

      // TODO: Validate that these configs are all still used.
      // TODO: Centralize default config values in a single place
      PropertyBuilder builder = new PropertyBuilder()
              .put(clusterProps.toProperties())
              .put(VeniceControllerClusterConfig.KAFKA_REPLICA_FACTOR, 1)
              .put(VeniceControllerClusterConfig.KAFKA_ZK_ADDRESS, zkAddress)
              .put(VeniceControllerClusterConfig.CONTROLLER_NAME, "venice-controller") // Why is this configurable?
              .put(VeniceControllerClusterConfig.REPLICA_FACTOR, 1)
              .put(VeniceControllerClusterConfig.NUMBER_OF_PARTITION, 1)
              .put(ConfigKeys.ADMIN_PORT, adminPort);

      VeniceProperties props = builder.build();

      VeniceController veniceController = new VeniceController(props);
      return new VeniceControllerWrapper(serviceName, dataDirectory, veniceController, adminPort);
    };
  }

  @Override
  public String getHost() {
    return DEFAULT_HOST_NAME;
  }

  @Override
  public int getPort() {
    return port;
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
   * @return the kafka topic name of the newly-created store-version
   */
  public String getNewStoreVersion(String clusterName) {
    String controllerUrl = getControllerUrl();
    String storeName = TestUtils.getUniqueString("venice-store");
    String storeOwner = TestUtils.getUniqueString("store-owner");
    long storeSize = 10 * 1024 * 1024;
    String keySchema = "\"long\"";
    String valueSchema = "\"string\"";
    StoreCreationResponse newStore = ControllerClient.createStoreVersion(
        controllerUrl,
        clusterName,
        storeName,
        storeOwner,
        storeSize,
        keySchema,
        valueSchema);
    return newStore.getKafkaTopic();
  }

  /***
   * Sets a version to be active for a given store and version
   *
   * @param storeName
   * @param version
   */
  public void setActiveVersion(String clusterName, String storeName, int version){
    ControllerClient.overrideSetActiveVersion(getControllerUrl(), clusterName, storeName, version);
  }

  /***
   * Set a version to be active, parsing store name and version number from a kafka topic name
   * @param kafkaTopic
   */
  public void setActiveVersion(String clusterName, String kafkaTopic){
    String storeName = Version.parseStoreFromKafkaTopicName(kafkaTopic);
    int version = Version.parseVersionFromKafkaTopicName(kafkaTopic);
    setActiveVersion(clusterName, storeName, version);
  }
}
