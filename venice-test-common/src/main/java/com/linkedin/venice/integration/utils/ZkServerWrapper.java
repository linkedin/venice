package com.linkedin.venice.integration.utils;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;
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
  static final boolean useSingleton = true;
  private static final ConcurrentLinkedQueue<String> chroots = new ConcurrentLinkedQueue<>();
  private static volatile ZkServerWrapper singleton = null;
  private String chroot; // only used for singleton, never includes a leading slash

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
    if (useSingleton){

      return (String serviceName, int port, File dir) -> {
        if (singleton == null) {
          synchronized (ZkServerWrapper.class) {
            if (singleton == null) {
              try {
                singleton = createRealZkServerWrapper(serviceName, port, dir);
                singleton.start();
                //Auto close
                Runtime.getRuntime().addShutdownHook(new Thread() {
                  public void run(){
                    singleton.close();
                  }
                });
              } catch (Exception e) {
                singleton = null;
                throw e; //So port bind exceptions leverage ServiceFactory for retry
              }
            }
          }
        }
        String chroot = chroots.poll();
        if (null == chroot){
          chroots.addAll(addPathsToZk(getSingletonAddress(), 100));
          chroot = chroots.poll();
        }
        return new ZkServerWrapper(dir, chroot);
      };

    } else {
      return (String serviceName, int port, File dir) -> createRealZkServerWrapper(serviceName, port, dir);
    }
  }

  private static ZkServerWrapper createRealZkServerWrapper(String serviceName, int port, File dir) {
    LOGGER.info("Creating ZkServerWrapper on port: " + port);
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
    } catch(Exception e) {
      throw new RuntimeException(e);
    }

    final ServerConfig configuration = new ServerConfig();
    configuration.readFrom(quorumConfiguration);
    return new ZkServerWrapper(dir, configuration);
  }

  // Instance-level state and APIs
  private final ServerConfig configuration;
  private ZkThread zkThread;
  private ZooKeeperServer zkServer;

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

  private ZkServerWrapper(File dataDir, String chroot) {
    super(SERVICE_NAME, dataDir);
    if (!useSingleton){
      throw new RuntimeException("Do not use the singleton-based ZkServerWrapper constructor without enabling the useSingleton flag");
    } else {
      this.chroot = chroot;
      this.zkThread = null;
      this.zkServer = null;
      this.configuration = null;
    }
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
  public String getAddress(){
    if (useSingleton){
      return getSingletonAddress() + "/" + chroot;
    } else {
      return getHost() + ":" + getPort();
    }
  }

  private static String getSingletonAddress(){
    return singleton.getHost() + ":" + singleton.getPort();
  }

  @Override
  protected void internalStart() throws Exception {
    if (useSingleton && this != singleton){
      return;
    }
    long startTime = System.currentTimeMillis();
    zkThread.start();
    while (!zkServer.isRunning() && zkThread.exception == null) {
      long currentTime = System.currentTimeMillis();
      if ((currentTime - startTime) > MAX_WAIT_TIME_DURING_STARTUP) {
        close();
        throw new VeniceException("Unable to start ZK within the maximum allotted time (" + MAX_WAIT_TIME_DURING_STARTUP + " ms).");
      } else {
        Thread.sleep(100);
      }
    }

    if (zkThread.exception != null) {
      throw new VeniceException("ZooKeeper failed to start.", zkThread.exception);
    }

    LOGGER.info("ZK is running: " + zkServer.isRunning());
  }

  @Override
  protected void internalStop() throws Exception {
    if (useSingleton){
      return;
    }
    zkServer.shutdown();
    zkThread.interrupt();
  }

  @Override
  protected void newProcess()
      throws Exception {
    if (useSingleton){
      throw new RuntimeException("newProcess is not implemented for singleton ZkServerWrappers");
    }
    this.zkServer = new ZooKeeperServer();
    this.zkThread = new ZkThread();
  }

  /**
   * Creating a connection to zookeeper to add a path takes about as long as creating a Zk instance, so we need to add
   * available chroots in bulk in order to get any performance improvements from reusing the same Zk instance.
   *
   * @param zkConnection
   * @param count how many random root paths to create
   * @return List of paths that were created
   */
  private static List<String> addPathsToZk(String zkConnection, int count){
    List<String> addedPaths = new ArrayList<>();
    try {
      ZooKeeper zooKeeper = new ZooKeeper(zkConnection, 10000, event -> {});
      TestUtils.waitForNonDeterministicCompletion(5, TimeUnit.SECONDS, () -> {
        return zooKeeper.getState().equals(ZooKeeper.States.CONNECTED);
      });
      for (int i=0; i< count; i++) {
        String path = TestUtils.getUniqueString("test");
        zooKeeper.create("/"+path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        addedPaths.add(path);
      }
      zooKeeper.close();
    } catch (Exception e){
      throw new RuntimeException("Failed to create paths on zookeeper at: " + zkConnection, e);
    }
    return addedPaths;
  }
}
