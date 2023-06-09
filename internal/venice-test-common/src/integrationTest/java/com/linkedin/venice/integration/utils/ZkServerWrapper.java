package com.linkedin.venice.integration.utils;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;


/**
 * This class contains a ZK server, and provides facilities for cleaning up
 * its side effects when we're done using it.
 */
public class ZkServerWrapper extends ProcessWrapper {
  // Class-level state and APIs
  private static final Logger LOGGER = LogManager.getLogger(ZkServerWrapper.class);

  public static final String SERVICE_NAME = "Zookeeper";
  private static final int MAX_WAIT_TIME_DURING_STARTUP = 5 * Time.MS_PER_SECOND;
  private static final ConcurrentLinkedQueue<String> CHROOTS = new ConcurrentLinkedQueue<>();

  /**
   * The singleton ZK that all wrappers end up using
   */
  private static ZkServerWrapper INSTANCE = null;
  private static ZooKeeper zooKeeper = null;

  // isSingleton indicates if a singleton ZkServer thread is created and shared among all test suits.
  private static boolean isSingleton = false;

  // TODO: Make sure the hardcoded defaults below make sense

  /**
   * The tick time can be low because this Zookeeper instance is intended to be used locally.
   */
  private static final int TICK_TIME = 200;
  private static final int MAX_SESSION_TIMEOUT = 10 * Time.MS_PER_SECOND;
  private static final int NUM_CONNECTIONS = 5000;

  private static final String CLIENT_PORT_PROP = "clientPort";
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
    return (String serviceName, File dataDirectory) -> {
      if (!isSingleton) {
        return createRealZkServerWrapper(serviceName, TestUtils.getFreePort(), dataDirectory);
      }

      synchronized (ZkServerWrapper.class) {
        if (INSTANCE == null) {
          try {
            INSTANCE = createRealZkServerWrapper(serviceName, TestUtils.getFreePort(), dataDirectory);
            Runtime.getRuntime().addShutdownHook(new Thread(INSTANCE::close));
            INSTANCE.start();
          } catch (Exception e) {
            INSTANCE = null;
            throw e;
          }
        }
      }

      String chroot = CHROOTS.poll();
      if (chroot == null) {
        CHROOTS.addAll(addPathsToZk(INSTANCE.getAddress(), 1));
        chroot = CHROOTS.poll();
      }
      return new ZkServerWrapper(dataDirectory, chroot);
    };
  }

  private static ZkServerWrapper createRealZkServerWrapper(String serviceName, int port, File dir) {
    LOGGER.info("Creating ZkServerWrapper on port: {}", port);
    Properties startupProperties = new Properties();

    // Static configs
    startupProperties.setProperty(TICK_TIME_PROP, Integer.toString(TICK_TIME));
    startupProperties.setProperty(MAX_SESSION_TIMEOUT_PROP, Integer.toString(MAX_SESSION_TIMEOUT));
    startupProperties.setProperty(NUM_CONNECTIONS_PROP, Integer.toString(NUM_CONNECTIONS));

    // Dynamic configs
    startupProperties.setProperty(CLIENT_PORT_PROP, Integer.toString(port));
    startupProperties.setProperty(DATA_DIR_PROP, dir.getAbsolutePath());

    QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
    try {
      quorumConfiguration.parseProperties(startupProperties);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    ServerConfig configuration = new ServerConfig();
    configuration.readFrom(quorumConfiguration);
    return new ZkServerWrapper(dir, configuration);
  }

  // Instance-level state and APIs
  private final String chroot; // only used for singleton, never includes a leading slash
  private final ServerConfig configuration;
  private ZkThread zkThread;
  private ZooKeeperServer zkServer;

  /**
   * Constructor for the singleton {@link #INSTANCE}.
   *
   * The constructor is private because {@link #generateService()} should be the only
   * way to construct a {@link ZkServerWrapper} instance.
   *
   * @param dataDirectory where ZK keeps its write ahead log and snapshot.
   * @param configuration the ZK ServerConfig
   */
  private ZkServerWrapper(File dataDirectory, ServerConfig configuration) {
    super(SERVICE_NAME, dataDirectory);
    this.chroot = null;
    this.zkThread = new ZkThread();
    this.zkServer = new ZooKeeperServer();
    this.configuration = configuration;
  }

  /**
   * Constructor for the wrappers handed out to the rest of the code.
   */
  private ZkServerWrapper(File dataDirectory, String chroot) {
    super(SERVICE_NAME, dataDirectory);
    this.chroot = chroot;
    this.zkThread = null;
    this.zkServer = null;
    this.configuration = null;
  }

  /**
   * Adapted from ZK's source code. We need to be able to block until ZK has successfully started.
   */
  private class ZkThread extends Thread {
    volatile Exception exception = null;

    public void run() {
      try {
        LOGGER.info("Starting ZK server");
        FileTxnSnapLog txnLog = null;
        try {
          txnLog = new FileTxnSnapLog(configuration.getDataLogDir(), configuration.getDataDir());
          zkServer.setTxnLogFactory(txnLog);
          zkServer.setTickTime(configuration.getTickTime());
          zkServer.setMinSessionTimeout(configuration.getMinSessionTimeout());
          zkServer.setMaxSessionTimeout(configuration.getMaxSessionTimeout());
          ServerCnxnFactory cnxnFactory = ServerCnxnFactory.createFactory();
          cnxnFactory.configure(configuration.getClientPortAddress(), configuration.getMaxClientCnxns());
          cnxnFactory.startup(zkServer);
          cnxnFactory.join();
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
    return "localhost";
  }

  /**
   * @see {@link ProcessWrapper#getPort()}
   */
  public int getPort() {
    if (isSingleton && this != INSTANCE) {
      return INSTANCE.getPort();
    }
    return configuration.getClientPortAddress().getPort();
  }

  @Override
  public String getAddress() {
    if (isSingleton && this != INSTANCE) {
      return INSTANCE.getAddress() + "/" + chroot;
    }
    return getHost() + ":" + getPort();
  }

  public static boolean isSingleton() {
    return isSingleton;
  }

  @Override
  protected void internalStart() throws Exception {
    if (isSingleton && this != INSTANCE) {
      return;
    }

    synchronized (ZkServerWrapper.class) {
      long expirationTime = System.currentTimeMillis() + MAX_WAIT_TIME_DURING_STARTUP;
      zkThread.start();
      while (!zkServer.isRunning() && zkThread.exception == null) {
        if (expirationTime < System.currentTimeMillis()) {
          close();
          throw new VeniceException(
              "Unable to start ZK within the maximum allotted time (" + MAX_WAIT_TIME_DURING_STARTUP + " ms).");
        }
        Thread.sleep(100);
      }

      if (zkThread.exception != null) {
        INSTANCE = null;
        throw new VeniceException("ZooKeeper failed to start.", zkThread.exception);
      }

      LOGGER.info("ZK is running: {}", zkServer.isRunning());
    }
  }

  @Override
  protected synchronized void internalStop() throws Exception {
    if (isSingleton && this != INSTANCE) {
      return;
    }
    synchronized (ZkServerWrapper.class) {
      zkThread.interrupt();
      zkThread.join();
      INSTANCE = null;
    }
  }

  @Override
  protected void newProcess() throws Exception {
    if (isSingleton) {
      throw new RuntimeException("newProcess is not implemented for singleton ZkServerWrappers");
    }
    this.zkThread = new ZkThread();
    this.zkServer = new ZooKeeperServer();
  }

  /**
   * Creating a connection to zookeeper to add a path takes about as long as creating a Zk instance, so we need to add
   * available chroots in bulk in order to get any performance improvements from reusing the same Zk instance.
   *
   * @param zkConnection
   * @param count how many random root paths to create
   * @return List of paths that were created
   */
  private synchronized static List<String> addPathsToZk(String zkConnection, int count) {
    if (zooKeeper != null && zooKeeper.getState() != ZooKeeper.States.CONNECTED) {
      try {
        zooKeeper.close();
        zooKeeper = null;
      } catch (InterruptedException e) {
        throw new VeniceException(e);
      }
    }
    if (zooKeeper == null) {
      try {
        zooKeeper = new ZooKeeper(zkConnection, MAX_SESSION_TIMEOUT, event -> {});
        TestUtils.waitForNonDeterministicCompletion(
            5,
            TimeUnit.SECONDS,
            () -> zooKeeper.getState().equals(ZooKeeper.States.CONNECTED));
      } catch (Exception e) {
        throw new RuntimeException("Failed to initialize ZK client: " + zkConnection, e);
      }
    }
    List<String> paths = new ArrayList<>(count);
    for (int i = 0; i < count; ++i) {
      String path = Utils.getUniqueString("test");
      try {
        zooKeeper.create("/" + path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      } catch (Exception e) {
        throw new RuntimeException("Failed to create paths on zookeeper at: " + zkConnection, e);
      }
      paths.add(path);
    }
    return paths;
  }

  @Override
  public String toString() {
    return "ZkService[" + getServiceName() + "@" + getHost() + ":" + getPort() + "]";
  }
}
