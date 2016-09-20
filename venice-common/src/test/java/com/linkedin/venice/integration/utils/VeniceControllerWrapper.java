package com.linkedin.venice.integration.utils;

import com.linkedin.venice.controller.VeniceController;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;

import java.io.File;

import static com.linkedin.venice.ConfigKeys.*;


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

  static StatefulServiceProvider<VeniceControllerWrapper> generateService(String clusterName, KafkaBrokerWrapper kafkaBrokerWrapper, boolean isParent) {
    // TODO: Once the ZK address used by Controller and Kafka are decoupled, change this
    String zkAddress = kafkaBrokerWrapper.getZkAddress();

    return (serviceName, port, dataDirectory) -> {
      VeniceProperties clusterProps = IntegrationTestUtils.getClusterProps(clusterName, dataDirectory, kafkaBrokerWrapper);

      int adminPort = IntegrationTestUtils.getFreePort();

      // TODO: Validate that these configs are all still used.
      // TODO: Centralize default config values in a single place
      PropertyBuilder builder = new PropertyBuilder()
              .put(clusterProps.toProperties())
              .put(KAFKA_REPLICA_FACTOR, 1)
              .put(KAFKA_ZK_ADDRESS, zkAddress)
              .put(CONTROLLER_NAME, "venice-controller") // Why is this configurable?
              .put(DEFAULT_REPLICA_FACTOR, 1)
              .put(DEFAULT_NUMBER_OF_PARTITION, 1)
              .put(ADMIN_PORT, adminPort)
              .put(DEFAULT_MAX_NUMBER_OF_PARTITIONS, 10)
              .put(DEFAULT_PARTITION_SIZE, 100)
              .put(TOPIC_MONITOR_POLL_INTERVAL_MS, 100)
              .put(CONTROLLER_PARENT_MODE, isParent);

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
   * @return VersionCreationResponse
   */
  public VersionCreationResponse getNewStoreVersion(String routerUrl, String clusterName) {
    String storeName = TestUtils.getUniqueString("venice-store");
    String storeOwner = TestUtils.getUniqueString("store-owner");
    long storeSize = 10 * 1024 * 1024;
    String keySchema = "\"string\"";
    String valueSchema = "\"string\"";
    // Create new store
    ControllerClient.createNewStore(
        routerUrl,
        clusterName,
        storeName,
        storeOwner,
        keySchema,
        valueSchema
    );
    // Create new version
    VersionCreationResponse newVersion = ControllerClient.createNewStoreVersion(
        routerUrl,
        clusterName,
        storeName,
        storeSize);
    if (newVersion.isError()){
      throw new VeniceException(newVersion.getError());
    }
    return newVersion;
  }

  /***
   * Sets a version to be active for a given store and version
   *
   * @param storeName
   * @param version
   */
  public void setActiveVersion(String routerUrl, String clusterName, String storeName, int version){
    ControllerClient.overrideSetActiveVersion(routerUrl, clusterName, storeName, version);
  }

  /***
   * Set a version to be active, parsing store name and version number from a kafka topic name
   * @param kafkaTopic
   */
  public void setActiveVersion(String routerUrl, String clusterName, String kafkaTopic){
    String storeName = Version.parseStoreFromKafkaTopicName(kafkaTopic);
    int version = Version.parseVersionFromKafkaTopicName(kafkaTopic);
    setActiveVersion(routerUrl, clusterName, storeName, version);
  }
}
