package com.linkedin.venice.integration.utils;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.VeniceProperties;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

import static com.linkedin.venice.ConfigKeys.*;


/**
 * Utility class to help with integration tests.
 *
 * N.B.: The visibility of this class and its functions is package-private on purpose.
 */
public class IntegrationTestUtils {
  static final int MAX_ASYNC_START_WAIT_TIME_MS = 30 * Time.MS_PER_SECOND;

  /**
   * N.B.: Visibility is package-private on purpose.
   */
  static VeniceProperties getClusterProps(String clusterName, File dataDirectory, String zkAddress,
      KafkaBrokerWrapper kafkaBrokerWrapper, boolean sslToKafka) {
    // TODO: Validate that these configs are all still used.
    // TODO: Centralize default config values in a single place

    VeniceProperties clusterProperties = new PropertyBuilder()

        // Helix-related config
        .put(ZOOKEEPER_ADDRESS, zkAddress)

        // Kafka-related config
        .put(KAFKA_CONSUMER_FETCH_BUFFER_SIZE, 65536)
        .put(KAFKA_CONSUMER_SOCKET_TIMEOUT_MS, 100)
        .put(KAFKA_CONSUMER_NUM_METADATA_REFRESH_RETRIES, 3)
        .put(KAFKA_CONSUMER_METADATA_REFRESH_BACKOFF_MS, 1000)
        .put(KAFKA_BOOTSTRAP_SERVERS, sslToKafka ? kafkaBrokerWrapper.getSSLAddress() : kafkaBrokerWrapper.getAddress())
        .put(KAFKA_ZK_ADDRESS, kafkaBrokerWrapper.getZkAddress())
        .put(KAFKA_REQUEST_TIMEOUT_MS, 5000)
        .put(KAFKA_DELIVERY_TIMEOUT_MS, 5000)
        .put(KAFKA_MAX_BLOCK_MS, 60000)

        // Other configs
        .put(CLUSTER_NAME, clusterName)
        .put(OFFSET_MANAGER_TYPE, PersistenceType.ROCKS_DB.toString())
        .put(OFFSET_MANAGER_FLUSH_INTERVAL_MS, 1000)
        .put(OFFSET_DATA_BASE_PATH, dataDirectory.getAbsolutePath())
        .put(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB.toString())
        .put(CONTROLLER_ADD_VERSION_VIA_ADMIN_PROTOCOL, false)
        .build();

    return clusterProperties;
  }

  /**
   * N.B.: Visibility is package-private on purpose.
   *
   * @param directory where to create the file
   * @param fileName for the new config file
   * @param content of the new config file
   * @return a handle on the newly-created config file.
   */
  static File getConfigFile(File directory, String fileName, Properties content) {
    if (!directory.exists()) {
      directory.mkdir();
    } else if (!directory.isDirectory()) {
      throw new VeniceException("Can only create a config file in a directory, not in a file: " + directory.getAbsolutePath());
    }
    File propsFile = new File(directory, fileName);
    if (propsFile.exists()) {
      throw new VeniceException("The file already exists: " + propsFile.getAbsolutePath());
    }
    try {
      propsFile.createNewFile();
      try (FileWriter writer = new FileWriter(propsFile)) {
        content.store(writer, "Config file: " + fileName);
      }
    } catch (IOException e) {
      throw new VeniceException("Got an IOExpcetion while trying to create or write to the file: " + propsFile.getAbsolutePath(), e);
    }
    return propsFile;
  }
}
