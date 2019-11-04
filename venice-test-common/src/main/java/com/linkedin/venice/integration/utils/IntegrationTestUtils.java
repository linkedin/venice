package com.linkedin.venice.integration.utils;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.PersistenceType;

import static com.linkedin.venice.ConfigKeys.*;

import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Properties;


/**
 * Utility class to help with integration tests.
 *
 * N.B.: The visibility of this class and its functions is package-private on purpose.
 */
public class IntegrationTestUtils {
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
    String tmpDirectory = System.getProperty(TestUtils.TEMP_DIRECTORY_SYSTEM_PROPERTY);
    String directoryName = TestUtils.getUniqueString("VeniceTemp-" + serviceName + "-Data");
    File dataDir = new File(tmpDirectory, directoryName).getAbsoluteFile();
    dataDir.deleteOnExit(); //Doesn't always work
    return dataDir;
  }

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

        // Other configs
        .put(CLUSTER_NAME, clusterName)
        .put(OFFSET_MANAGER_TYPE, PersistenceType.BDB.toString())
        .put(OFFSET_MANAGER_FLUSH_INTERVAL_MS, 1000)
        .put(OFFSET_DATA_BASE_PATH, dataDirectory.getAbsolutePath())
        .put(PERSISTENCE_TYPE, PersistenceType.BDB.toString())
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
      content.store(new FileWriter(propsFile), "Config file: " + fileName);
    } catch (IOException e) {
      throw new VeniceException("Fot an IOExpcetion while trying to create or write to the file: " + propsFile.getAbsolutePath(), e);
    }
    return propsFile;
  }
}
