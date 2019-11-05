package com.linkedin.venice.etl.publisher;

import azkaban.jobExecutor.AbstractJob;
import java.util.concurrent.TimeUnit;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.etl.client.VeniceKafkaConsumerClient;
import com.linkedin.venice.exceptions.UndefinedPropertyException;
import com.linkedin.venice.meta.StoreInfo;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.api.JobExecutionDriver;
import org.apache.gobblin.runtime.embedded.EmbeddedGobblinDistcp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.Logger;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.etl.source.VeniceKafkaSource.*;


/**
 * Venice ETL publisher is an Azkaban workflow that will publish the latest ETL snapshot
 * for Venice stores to a specified HDFS path.
 *
 * In the snapshot source directory, Venice ETL will dump snapshots for different store
 * versions; different versions will end up in different directories. Publisher will talk
 * to Venice controller to figure out the current version of the stores and copy the latest
 * snapshot of the current version to the specified destination path.
 *
 * Publisher workflow can run asynchronously with other ETL workflow; publisher will skip
 * doing duplicate work if the latest snapshot is published already.
 */
public class VeniceETLPublisher extends AbstractJob {
  private static final Logger logger = Logger.getLogger(VeniceETLPublisher.class);

  /**
   * Any job configs that start with this prefix will be applied on the {@link EmbeddedGobblinDistcp} job.
   */
  private static final String DISTCP_CONFIG_PREFIX = "venice.distcp.";

  /**
   * Any stores which enables future etl will have this suffix appended to job path.
   */
  private static final String FUTURE_ETL_SUFFIX = "_future";

  /**
   * Daily ETL snapshot format is "timestamp-PT-recordNumber-versionNumber"
   */
  private static final String SNAPSHOT_SEPARATOR = "-PT-";
  /**
   * The regex is "\A[0-9]+-PT-[0-9]+\z", which matches the strings like:
   * 1570183273436-PT-558022380
   * 1570269674164-PT-558132495
   */
  private static final Pattern SNAPSHOT_PATTERN = Pattern.compile("\\A[0-9]+" + SNAPSHOT_SEPARATOR + "[0-9]+\\z");

  /**
   * Hadoop Path comparator which puts bigger path in the beginning, so the PriorityQueue will be a max heap
   * and the first element will be the latest snapshot.
   */
  private static final Comparator<Path> SNAPSHOT_COMPARATOR = new Comparator<Path>() {
    @Override
    public int compare(Path o1, Path o2) {
      // Bigger path comes first
      return -1 * o1.compareTo(o2);
    }
  };
  private static int INITIAL_QUEUE_SIZE = 10;
  private static int DEFAULT_MAX_SNAPSHOTS_TO_KEEP = 3;

  // Immutable state
  private final VeniceProperties props;
  private final String veniceControllerUrls;
  private final int maxDailySnapshotsToKeep;
  private final String snapshotSourceDir;
  private final String snapshotDestinationDirPrefix;

  private FileSystem fs;
  private Map<Pair<String, String>, JobExecutionDriver> distcpJobFutures;
  private Map<String, String> ETLStoreToUserName;
  private Set<String> futureETLEnabledStores;

  public VeniceETLPublisher(String jobId, Properties vanillaProps) throws Exception{
    super(jobId, logger);
    this.props = new VeniceProperties(vanillaProps);
    logger.info("Constructing " + VeniceETLPublisher.class.getSimpleName() + ": " + props.toString(true));
    this.veniceControllerUrls = props.getString(VENICE_CHILD_CONTROLLER_URLS);
    this.maxDailySnapshotsToKeep = props.getInt(ETL_MAX_SNAPSHOTS_TO_KEEP, DEFAULT_MAX_SNAPSHOTS_TO_KEEP);

    // the directory that contains the snapshots generated by Gobblin/Lumos pipeline,
    // which are the snapshots in version level
    this.snapshotSourceDir = props.getString(ETL_SNAPSHOT_SOURCE_DIR);
    // the directory where we publish the latest snapshot, and in the destination, we only specify store name in the path.
    this.snapshotDestinationDirPrefix = props.getString(ETL_SNAPSHOT_DESTINATION_DIR_PREFIX);

    // gets configured HDFS filesystem for read and write
    try {
      this.fs = FileSystem.get(new Configuration());
    } catch (IOException e) {
      logger.error("Get configured FileSystem implementation failed: ", e);
      throw e;
    }

    // records all distcp job futures
    this.distcpJobFutures = new HashMap<>();

    // builds the map from etl store name to venice etl user name
    this.ETLStoreToUserName = new HashMap<>();
    setupETLStoreToUserName(props, ETLStoreToUserName);

    // builds the set for future version etl enabled stores
    this.futureETLEnabledStores = new HashSet<>();
    setUpFutureETLStores(props, futureETLEnabledStores);
  }

  @Override
  public void run() throws Exception {
    // Get all latest ETL snapshots from the source directory
    Map<String, StoreETLSnapshotInfo> storeToSnapshotPath = getSnapshotPath(snapshotSourceDir);

    // Build ControllerClient, which will be used to determine the current version and potentially future version of each store
    Map<String, ControllerClient> storeToControllerClient = VeniceKafkaConsumerClient.getControllerClients(
        storeToSnapshotPath.keySet(), this.veniceControllerUrls);

    for (Map.Entry<String, StoreETLSnapshotInfo> entry : storeToSnapshotPath.entrySet()) {
      String storeName = entry.getKey();
      StoreETLSnapshotInfo snapshotInfo = entry.getValue();
      StoreResponse storeResponse = storeToControllerClient.get(storeName).getStore(storeName);
      StoreInfo storeInfo = storeResponse.getStore();

      // Get current version of the store and publish current version snapshots
      int currentVersion = storeInfo.getCurrentVersion();
      logger.info("Current version for store: " + storeName + " is " + currentVersion);
      publishVersionSnapshots(storeName, snapshotInfo, currentVersion);

      // Get future version and publish future version snapshots
      if (futureETLEnabledStores.contains(storeName)) {
        int futureVersion = snapshotInfo.getLargestVersionNumber();
        if (futureVersion > currentVersion) {
          logger.info("Future version for store: " + storeName + " is " + futureVersion);
          publishVersionSnapshots(storeName + FUTURE_ETL_SUFFIX, snapshotInfo, futureVersion);
        } else {
          logger.info("Store " + storeName + " doesn't have a future version running yet. Skipped publishing.");
        }
      }
    }

    /**
     * Wait for each job future to return. It keeps looping the jobs until all of them have finished.
     */
    while(!distcpJobFutures.entrySet().isEmpty()) {
      Iterator<Map.Entry<Pair<String, String>, JobExecutionDriver>> itr = distcpJobFutures.entrySet().iterator();
      while (itr.hasNext()) {
        Map.Entry<Pair<String, String>, JobExecutionDriver> jobEntry = itr.next();
        String storeName = jobEntry.getKey().getFirst();
        String destination = jobEntry.getKey().getSecond();
        JobState.RunningState jobState = jobEntry.getValue().getJobExecutionState().getRunningState();
        if (jobState.isSuccess()) {
          logger.info("Successfully published the latest snapshot for store " + storeName + " in " + destination);
          itr.remove();
        } else if (jobState.isFailure()) {
          logger.info("Distcp job failed for store " + storeName, jobEntry.getValue().get().getErrorCause());
          itr.remove();
        } else if (jobState.isCancelled()) {
          logger.info("Distcp job canceled for store " + storeName + " in " + destination);
          itr.remove();
        } else {
          //The jon is still running or pending, sleep for 1s
          logger.info("Distcp job for store " + storeName + "is still ongoing.");
          TimeUnit.MINUTES.sleep(1);
        }
      }
    }
    this.distcpJobFutures.clear();
  }

  private void publishVersionSnapshots(String storeNamePath, StoreETLSnapshotInfo snapshotInfo, int storeVersion) throws Exception {
    try {
      // build destination path
      String destination = snapshotDestinationDirPrefix;
      if (ETLStoreToUserName.containsKey(storeNamePath)) {
        destination += ETLStoreToUserName.get(storeNamePath);
      } else {
        destination += props.getString(ETL_STORE_TO_ACCOUNT_NAME_DEFAULT);
      }
      destination += "/" + storeNamePath;
      logger.info("Publishing ETL snapshot for store: " + storeNamePath + " to dir: " + destination);

      // Get snapshot list for the store version
      PriorityQueue<Path> snapshotQueue = snapshotInfo.getSnapshotsByVersion(storeVersion);
      if (null == snapshotQueue || snapshotQueue.size() == 0) {
        logger.info("No snapshot for store: " + storeNamePath);
        return;
      }

      // Get the latest snapshot from the sorted snapshot list
      Path snapshotSourcePath = snapshotQueue.peek();
      String snapshotName = snapshotSourcePath.getName();

      // Create the directory for the store in destination if it doesn't exist before
      Path storePath = new Path(destination);
      if (!fs.exists(storePath)) {
        fs.mkdirs(storePath);
      }

      // check whether the last snapshot already exists in the publish directory
      destination = destination + "/" + snapshotName + "_v" + storeVersion;
      Path snapshotDestinationPath = new Path(destination);
      if (fs.exists(snapshotDestinationPath)) {
        logger.info(
            "Skip copying snapshot " + snapshotName + " for store " + storeNamePath + ", because it already exists in " + destination);
        return;
      }

      /**
       * Start a Gobblin distributed cp job asynchronously to release snapshot;
       * Distcp for different stores will run in parallel;
       * Gobblin distcp is a MapReduce job which can speed up the data movement process a lot.
       */
      logger.info("Starting distcp for store " + storeNamePath);
      EmbeddedGobblinDistcp distcp = new EmbeddedGobblinDistcp(snapshotSourcePath, snapshotDestinationPath);
      // Add customized configs to the distcp job
      setupConfigs(distcp, this.props);
      JobExecutionDriver jobFuture = distcp.runAsync();
      distcpJobFutures.put(Pair.create(storeNamePath, destination), jobFuture);
      logger.info("Started distcp for store " + storeNamePath + " snapshot " + snapshotName);

      // clean up old snapshots in source
      retireOldSnapshots(snapshotQueue);
      // clean up old snapshots in destination
      retireDestinationOldSnapshots(storePath);
    } catch (Exception e) {
      logger.error("Failed on file operations: ", e);
      throw e;
    }
  }

  /**
   * Delete old snapshots in destination.
   * @param storePath
   * @throws IOException
   */
  private void retireDestinationOldSnapshots(Path storePath) throws IOException{
    FileStatus[] fileStatuses = fs.listStatus(storePath, PATH_FILTER);
    PriorityQueue<Path> snapshotQueue = new PriorityQueue<>(INITIAL_QUEUE_SIZE, SNAPSHOT_COMPARATOR);
    for (FileStatus fileStatus: fileStatuses) {
      snapshotQueue.add(fileStatus.getPath());
    }
    retireOldSnapshots(snapshotQueue);
  }

  /**
   * Delete all old snapshots.
   * After calling this method, snapshotQueue will be empty.
   */
  private void retireOldSnapshots(PriorityQueue<Path> snapshotQueue) {
    int latestSnapshotCounter = 0;
    try {
      while (!snapshotQueue.isEmpty()) {
        if (latestSnapshotCounter++ < maxDailySnapshotsToKeep) {
          // keep some latest snapshots
          snapshotQueue.poll();
        } else {
          Path retiredSnapshot = snapshotQueue.poll();
          if (fs.delete(retiredSnapshot, true)) {
            logger.info("Cleaned up snapshot " + retiredSnapshot.toUri().getRawPath());
          } else {
            logger.error("Failed to clean up snapshot " + retiredSnapshot.toUri().getRawPath());
          }
        }
      }
    } catch (Exception e) {
      logger.error("Error when cleaning up old snapshots", e);
    }
  }

  /**
   * The file structure in the snapshot source directory:
   * /path/to/source/storeName_version/timestamp-PT-recordNumber/ListOfAvroFiles
   *
   * The input snapshotsPath points to "/path/to/source/" in the above example,
   * this helper function will parse the snapshot path and return the state in
   * structured format:
   *              | version_10 -> list of snapshot paths sorted by timestamp
   * storeName -> | version_11 -> list of snapshot paths sorted by timestamp
   *              | ....
   */
  private Map<String, StoreETLSnapshotInfo> getSnapshotPath(String snapshotsPath)
        throws IOException {
    Map<String, StoreETLSnapshotInfo> snapshotInfoMap = new HashMap<>();
    Path path = new Path(snapshotsPath);
    FileStatus[] fileStatuses = fs.listStatus(path, PATH_FILTER);
    for (FileStatus fileStatus : fileStatuses) {
      if (!fileStatus.isDirectory()) {
        logger.info("Skipping " + fileStatus.getPath().getName());
        continue;
      }

      Path filePath = fileStatus.getPath();
      String storeVersion = filePath.getName();
      if (!Version.topicIsValidStoreVersion(storeVersion)) {
        logger.info("Skipping file path " + storeVersion + " because the table name doesn't contain version suffix");
        continue;
      }

      // build snapshot info collection for this store version
      String storeName = Version.parseStoreFromKafkaTopicName(storeVersion);
      int version = Version.parseVersionFromKafkaTopicName(storeVersion);
      StoreETLSnapshotInfo snapshotInfo = snapshotInfoMap.get(storeName);
      if (null == snapshotInfo) {
        snapshotInfo = new StoreETLSnapshotInfo(storeName);
        snapshotInfoMap.put(storeName, snapshotInfo);
      }

      // add all snapshots of this version to the list
      FileStatus[] snapshotStatuses = fs.listStatus(filePath, PATH_FILTER);
      for (FileStatus snapshotStatus : snapshotStatuses) {
        if (!snapshotStatus.isDirectory()) {
          logger.info("Skipping " + snapshotStatus.toString() + " because it's not a snapshot directory");
          continue;
        }

        Path snapshot = snapshotStatus.getPath();
        if (isValidSnapshotPath(snapshot.getName())) {
          snapshotInfo.addSnapshot(version, snapshot);
        }
      }
    }
    return snapshotInfoMap;
  }

  /**
   * A helper function to check whether the input string follow such
   * @param path
   * @return
   */
  public static boolean isValidSnapshotPath(String path) {
    Matcher m = SNAPSHOT_PATTERN.matcher(path);
    return m.matches();
  }

  /**
   * Helper function that adds customized configs to Gobblin distcp job.
   */
  private void setupConfigs(EmbeddedGobblinDistcp distcp, VeniceProperties properties) {
    for (String configKey : properties.keySet()) {
      if (configKey.startsWith(DISTCP_CONFIG_PREFIX)) {
        /**
         * The prefix is used to distinguish the distcp configs from VeniceETLPublisher job config,
         * remove the prefix before applying it on distcp
         */
        String conf = configKey.substring(DISTCP_CONFIG_PREFIX.length());
        distcp.setConfiguration(conf, properties.getString(configKey));
        logger.info("Added config " + conf + " with value " + properties.getString(configKey) + " to distcp");
      }
    }
  }

  /**
   * Helper function that builds the map from etl store name to user proxy name.
   */
  private static void setupETLStoreToUserName(VeniceProperties props, Map<String, String> ETLStoreToUserName) {
    try {
      /**
       * In the format of:
       * "{storeName1}:{proxyUser1};{storeName2}:{proxyUser2}..."
       */
      String etlStoreToUserName;
      try {
        etlStoreToUserName = props.getString(ETL_STORE_TO_ACCOUNT_NAME);
      } catch (UndefinedPropertyException e) {
        logger.error("The config of etl-store-to-account-name doesn't exist, will use default job path for all stores.");
        return;
      }
      String[] tokens = etlStoreToUserName.split(";");
      for (String token : tokens) {
        String[] storeNameAndUserName = token.split(":");
        ETLStoreToUserName.put(storeNameAndUserName[0].trim(), storeNameAndUserName[1].trim());
      }
    } catch (Exception e) {
      logger.error("Failed on setting up ETL Store to User Name: ", e);
      throw e;
    }
  }

  private static void setUpFutureETLStores(VeniceProperties props, Set<String> futureETLStores) {
    /**
     * In the format of:
     * "{storeName1},{storeName2}"
     */
    String futureETLEnabledStores;
    try {
      futureETLEnabledStores = props.getString(FUTURE_ETL_ENABLED_STORES);
    } catch (UndefinedPropertyException e) {
      logger.error("The config for future-etl-enabled-stores doesn't exist.");
      return;
    }
    String[] tokens = futureETLEnabledStores.split(",");
    for (String token : tokens) {
      futureETLStores.add(token.trim());
    }
  }

  /**
   * A helper class that stores the version to snapshot list mapping.
   */
  private static class StoreETLSnapshotInfo {
    String storeName;
    TreeMap<Integer, PriorityQueue<Path>> versionToSnapshotPath;

    StoreETLSnapshotInfo(String storeName) {
      this.storeName = storeName;
      // The versions are sorted for future etl
      this.versionToSnapshotPath = new TreeMap<>();
    }

    /**
     * Snapshot is added to list in sorted order for two reasons:
     * 1. Return the latest snapshot easily;
     * 2. Keep a list of snapshots instead of a single reference to latest snapshot for automatic clean up
     * @param version
     * @param snapshot
     */
    protected void addSnapshot(int version, Path snapshot) {
      PriorityQueue<Path> snapshotQueue = this.versionToSnapshotPath.get(version);
      if (null == snapshotQueue) {
        snapshotQueue = new PriorityQueue<>(INITIAL_QUEUE_SIZE, SNAPSHOT_COMPARATOR);
        this.versionToSnapshotPath.put(version, snapshotQueue);
      }
      snapshotQueue.offer(snapshot);
    }

    protected PriorityQueue<Path> getSnapshotsByVersion(int version) {
      return versionToSnapshotPath.get(version);
    }

    protected int getLargestVersionNumber() {
      return this.versionToSnapshotPath.lastKey();
    }
  }

  /**
   * ignore hdfs files with prefix "_" and "."
   */
  private static final PathFilter PATH_FILTER = p -> !p.getName().startsWith("_") && !p.getName().startsWith(".");
}
