package com.linkedin.venice.integration.utils;

import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.utils.*;

import static com.linkedin.venice.ConfigKeys.*;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;


/**
 * Utility class to help with integration tests.
 *
 * N.B.: The visibility of this class and its functions is package-private on purpose.
 */
class IntegrationTestUtils {
  static final int MAX_ASYNC_START_WAIT_TIME_MS = 10 * Time.MS_PER_SECOND;

  /**
   * WARNING: The code which generates the free port and uses it must always be called within
   * a try/catch and a loop. There is no guarantee that the port returned will still be
   * available at the time it is used. This is best-effort only.
   *
   * N.B.: Visibility is package-private on purpose.
   *
   * @return a free port to be used by tests.
   */
  static int getFreePort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    } catch(IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * N.B.: Visibility is package-private on purpose.
   */
  static File getDataDirectory(String serviceName) {
    String tmpDirectory = System.getProperty("java.io.tmpdir");
    String directoryName = TestUtils.getUniqueString(serviceName + "-Data");
    return new File(tmpDirectory, directoryName).getAbsoluteFile();
  }

  /**
   * N.B.: Visibility is package-private on purpose.
   */
  static VeniceProperties getClusterProps(String clusterName, File dataDirectory, KafkaBrokerWrapper kafkaBrokerWrapper) {
    // TODO: Validate that these configs are all still used.
    // TODO: Centralize default config values in a single place

    VeniceProperties clusterProperties = new PropertyBuilder()

        // Helix-related config
    .put(HELIX_ENABLED, true)
    .put(ZOOKEEPER_ADDRESS, kafkaBrokerWrapper.getZkAddress())

    // Kafka-related config
    .put(KAFKA_CONSUMER_FETCH_BUFFER_SIZE, 65536)
    .put(KAFKA_CONSUMER_SOCKET_TIMEOUT_MS, 100)
    .put(KAFKA_CONSUMER_NUM_METADATA_REFRESH_RETRIES, 3)
    .put(KAFKA_CONSUMER_METADATA_REFRESH_BACKOFF_MS, 1000)
    .put(KAFKA_BOOTSTRAP_SERVERS, kafkaBrokerWrapper.getAddress())
    .put(KAFKA_AUTO_COMMIT_INTERVAL_MS, 1000)

        // Other configs
    .put(CLUSTER_NAME, clusterName)
    .put(OFFSET_MANAGER_TYPE, PersistenceType.BDB.toString())
    .put(OFFSET_MANAGER_FLUSH_INTERVAL_MS, 1000)
    .put(OFFSET_DATA_BASE_PATH, dataDirectory.getAbsolutePath())
    .put(PERSISTENCE_TYPE, PersistenceType.BDB.toString()).build();

    return clusterProperties;
  }

}
