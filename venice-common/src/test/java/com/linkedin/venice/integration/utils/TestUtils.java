package com.linkedin.venice.integration.utils;

import com.linkedin.venice.utils.Props;
import com.linkedin.venice.utils.RandomGenUtils;

import static com.linkedin.venice.ConfigKeys.*;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;

/**
 * Utility class to get a free port.
 *
 * N.B.: The visibility of some of the functions in this class is package-private on purpose.
 */
public class TestUtils {
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
    String directoryName = getUniqueString(serviceName + "-Data");
    return new File(tmpDirectory, directoryName).getAbsoluteFile();
  }

  /**
   * N.B.: Visibility is package-private on purpose.
   */
  static Props getClusterProps(String clusterName, File dataDirectory, KafkaBrokerWrapper kafkaBrokerWrapper) {
    // TODO: Validate that these configs are all still used.
    // TODO: Centralize default config values in a single place

    Props clusterProps = new Props()

        // Helix-related config
        .with(HELIX_ENABLED, true)
        .with(ZOOKEEPER_ADDRESS, kafkaBrokerWrapper.getZkAddress())

        // Kafka-related config
        .with(KAFKA_CONSUMER_FETCH_BUFFER_SIZE, 65536)
        .with(KAFKA_CONSUMER_SOCKET_TIMEOUT_MS, 100)
        .with(KAFKA_CONSUMER_NUM_METADATA_REFRESH_RETRIES, 3)
        .with(KAFKA_CONSUMER_METADATA_REFRESH_BACKOFF_MS, 1000)
        .with(KAFKA_BOOTSTRAP_SERVERS, kafkaBrokerWrapper.getAddress())
        .with(KAFKA_AUTO_COMMIT_INTERVAL_MS, 1000)

        // Other configs
        .with(CLUSTER_NAME, clusterName)
        .with(OFFSET_MANAGER_TYPE, "bdb") // FIXME: Make this an enum
        .with(OFFSET_MANAGER_FLUSH_INTERVAL_MS, 1000)
        .with(OFFSET_DATA_BASE_PATH, dataDirectory.getAbsolutePath())
        .with(PERSISTENCE_TYPE, "BDB"); // FIXME: Make this an enum

    return clusterProps;
  }

  public static String getUniqueString(String base) {
    return base + "-" + System.currentTimeMillis() + "-" + RandomGenUtils.getRandomIntwithin(Integer.MAX_VALUE);
  }
}
