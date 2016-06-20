package com.linkedin.venice.integration.utils;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.Time;
import org.apache.log4j.Logger;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.io.File;
import java.util.Properties;

/**
 * This class contains a ZK server, and provides facilities for cleaning up
 * its side effects when we're done using it.
 */
public class ZkServerWrapper extends ProcessWrapper {
  // Class-level state and APIs

  private static final Logger LOGGER = Logger.getLogger(ZkServerWrapper.class);

  public static final String SERVICE_NAME = "Zookeeper";

  private static final int MAX_WAIT_TIME_DURING_STARTUP = 5 * Time.MS_PER_SECOND;

  // TODO: Make sure the hardcoded defaults below make sense

  /**
   * The tick time can be low because this Zookeeper instance is intended to be used locally.
   */
  private static final int TICK_TIME = 200;
  private static final int MAX_SESSION_TIMEOUT = 10 * Time.MS_PER_SECOND;
  private static final int NUM_CONNECTIONS = 5000;
  private static final String ZK_HOSTNAME = "localhost"; // Retrieving Utils.getHostName() doesn't work...

  private static final String CLIENT_PORT_PROP = "clientPort";
  private static final String CLIENT_PORT_ADDRESS = "clientPortAddress";
  private static final String TICK_TIME_PROP = "tickTime";
  private static final String MAX_SESSION_TIMEOUT_PROP = "maxSessionTimeout";
  private static final String NUM_CONNECTIONS_PROP = "maxClientCnxns";
  private static final String DATA_DIR_PROP = "dataDir";


  /**
   * This is package private because the only way to call this should be from
   * {@link ServiceFactory#getZkServer()}.
   *
   * @return a function which yields a {@link ZkServerWrapper} instance
   */
  static StatefulServiceProvider<ZkServerWrapper> generateService() {
    return (String serviceName, int port, File dir) -> {
      Properties startupProperties = new Properties();

      // Static configs
      startupProperties.setProperty(TICK_TIME_PROP, Integer.toString(TICK_TIME));
      startupProperties.setProperty(MAX_SESSION_TIMEOUT_PROP, Integer.toString(MAX_SESSION_TIMEOUT));
      startupProperties.setProperty(NUM_CONNECTIONS_PROP, Integer.toString(NUM_CONNECTIONS));

      // Dynamic configs
      startupProperties.setProperty(CLIENT_PORT_PROP, Integer.toString(port));
      startupProperties.setProperty(CLIENT_PORT_ADDRESS, ZK_HOSTNAME);
      startupProperties.setProperty(DATA_DIR_PROP, dir.getAbsolutePath());

      QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
      try {
        quorumConfiguration.parseProperties(startupProperties);
      } catch(Exception e) {
        throw new RuntimeException(e);
      }

      final ServerConfig configuration = new ServerConfig();
      configuration.readFrom(quorumConfiguration);
      return new ZkServerWrapper(dir, configuration);
    };
  }

  // Instance-level state and APIs
  private final ServerConfig configuration;
  private final ZkThread zkThread;
  private final ZooKeeperServer zkServer;

  /**
   * The constructor is private because {@link #generateService()} should be the only
   * way to construct a {@link ZkServerWrapper} instance.
   *
   * @param dataDirectory where ZK keeps its write ahead log and snapshot.
   * @param configuration the ZK ServerConfig
   */
  private ZkServerWrapper(File dataDirectory, ServerConfig configuration) {
    super(SERVICE_NAME, dataDirectory);
    this.configuration = configuration;
    this.zkServer = new ZooKeeperServer();
    this.zkThread = new ZkThread();
  }

  /**
   * Adapted from ZK's source code. We need to be able to block until ZK has successfully started.
   */
  private class ZkThread extends Thread {
    Exception exception = null;
    public void run() {
      try {
        LOGGER.info("Starting ZK server");
        FileTxnSnapLog txnLog = null;
        try {
          txnLog = new FileTxnSnapLog(new File(configuration.getDataLogDir()), new File(
              configuration.getDataDir()));
          zkServer.setTxnLogFactory(txnLog);
          zkServer.setTickTime(configuration.getTickTime());
          zkServer.setMinSessionTimeout(configuration.getMinSessionTimeout());
          zkServer.setMaxSessionTimeout(configuration.getMaxSessionTimeout());
          ServerCnxnFactory cnxnFactory = ServerCnxnFactory.createFactory();
          cnxnFactory.configure(configuration.getClientPortAddress(),
              configuration.getMaxClientCnxns());
          cnxnFactory.startup(zkServer);
          cnxnFactory.join();
        } catch (InterruptedException e) {
          // warn, but generally this is ok
          LOGGER.warn("ZkThread interrupted", e);
        } finally {
          if (zkServer.isRunning()) {
            zkServer.shutdown();
          }
          if (txnLog != null) {
            txnLog.close();
          }
        }
      } catch (Exception e) {
        exception = e;
      }
    }
  }

  /**
   * @see {@link ProcessWrapper#getHost()}
   */
  public String getHost() {
    return ZK_HOSTNAME;
  }

  /**
   * @see {@link ProcessWrapper#getPort()}
   */
  public int getPort() {
    return configuration.getClientPortAddress().getPort();
  }

  @Override
  public void start() throws Exception {
    long startTime = System.currentTimeMillis();
    zkThread.start();
    while (!zkServer.isRunning() && zkThread.exception == null) {
      long currentTime = System.currentTimeMillis();
      if ((currentTime - startTime) > MAX_WAIT_TIME_DURING_STARTUP) {
        stop();
        throw new VeniceException("Unable to start ZK within the maximum allotted time (" + MAX_WAIT_TIME_DURING_STARTUP + " ms).");
      } else {
        // Let's check again later
        Thread.sleep(100);
      }
    }

    if (zkThread.exception != null) {
      throw new VeniceException("ZooKeeper failed to start.", zkThread.exception);
    }

    LOGGER.info("ZK is running: " + zkServer.isRunning());
  }

  @Override
  public void stop() throws Exception {
    zkServer.shutdown();
    zkThread.interrupt();
  }
}
