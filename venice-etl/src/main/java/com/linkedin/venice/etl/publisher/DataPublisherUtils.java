package com.linkedin.venice.etl.publisher;

import com.linkedin.venice.exceptions.UndefinedPropertyException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Pair;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.gobblin.runtime.JobState;
import org.apache.gobblin.runtime.api.JobExecutionDriver;
import org.apache.gobblin.runtime.embedded.EmbeddedGobblinDistcp;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.Logger;

import static com.linkedin.venice.ConfigKeys.*;


public class DataPublisherUtils {

  private static final Logger logger = Logger.getLogger(DataPublisherUtils.class);

  /**
   * Any job configs that start with this prefix will be applied on the {@link EmbeddedGobblinDistcp} job.
   */
  private static final String DISTCP_CONFIG_PREFIX = "venice.distcp.";

  /**
   * Daily ETL snapshot format is "timestamp-PT-recordNumber"
   */
  private static final String SNAPSHOT_SEPARATOR = "-PT-";
  /**
   * The regex is "\A[0-9]+-PT-[0-9]+\z", which matches the strings like:
   * 1570183273436-PT-558022380
   * 1570269674164-PT-558132495
   */
  private static final Pattern SNAPSHOT_PATTERN = Pattern.compile("\\A[0-9]+" + SNAPSHOT_SEPARATOR + "[0-9]+\\z");

  /**
   * ignore hdfs files with prefix "_" and "."
   */
  protected static final PathFilter PATH_FILTER = p -> !p.getName().startsWith("_") && !p.getName().startsWith(".");

  protected static int DEFAULT_MAX_FILES_TO_KEEP = 3;

  /**
   * The file structure in the Lumos source directory:
   * /path/to/source/storeName_version/timestamp-PT-recordNumber/ListOfAvroFiles
   *
   * The input filesPath points to "/path/to/source/" in the above example,
   * this helper function will parse the snapshots path and return the state in
   * structured format:
   *              | version_10 -> list of snapshots paths (timestamp-PT-recordNumber files) sorted by timestamp
   * storeName -> | version_11 -> list of snapshots paths sorted by timestamp
   *              | ....
   */
  public static Map<String, StoreFilesInfo> getSnapshotsPath(String filesPath, FileSystem fs, Comparator<Path> comparator)
      throws IOException {
    Map<String, StoreFilesInfo> snapshotInfoMap = new HashMap<>();
    Path path = new Path(filesPath);
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
      StoreFilesInfo snapshotInfo = snapshotInfoMap.get(storeName);
      if (null == snapshotInfo) {
        snapshotInfo = new StoreFilesInfo(comparator);
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
          snapshotInfo.addFileForStoreVersion(version, snapshot);
        }
      }
    }
    return snapshotInfoMap;
  }

  /**
   * The file structure in the Gobblin source directory:
   * /path/to/source/storeName_version/hourly/year/month/day/hour/ListOfAvroFiles
   *
   * The input filesPath points to "/path/to/source/" in the above example,
   * this helper function will parse the snapshots path and return the state in
   * structured format:
   *              | version_10 -> list of .../hourly/year/month/day/hour/ sorted by timestamp
   * storeName -> | version_11 -> list of .../hourly/year/month/day/hour/ sorted by timestamp
   *              | ....
   */
  public static Map<String, StoreFilesInfo> getDeltasPath(String filesPath, FileSystem fs,
      Comparator comparator, Set<String> onDemandETLEnabledStores)
      throws IOException {
    Map<String, StoreFilesInfo> snapshotInfoMap = new HashMap<>();
    Path path = new Path(filesPath);
    FileStatus[] fileStatuses = fs.listStatus(path, PATH_FILTER);
    for (FileStatus fileStatus : fileStatuses) {
      if (!fileStatus.isDirectory()) {
        logger.info("Skipping " + fileStatus.getPath().getName());
        continue;
      }
      Path filePath = fileStatus.getPath();
      String storeVersion = filePath.getName();
      /**
       * gets store name and version
       */
      String storeName = Version.parseStoreFromKafkaTopicName(storeVersion);
      if (!onDemandETLEnabledStores.contains(storeName)) {
        continue;
      }
      int version = Version.parseVersionFromKafkaTopicName(storeVersion);
      StoreFilesInfo snapshotInfo = snapshotInfoMap.get(storeName);
      if (null == snapshotInfo) {
        snapshotInfo = new StoreFilesInfo(comparator);
        snapshotInfoMap.put(storeName, snapshotInfo);
      }
      /**
       * adds all .../hourly/year/month/day/hour/ paths for this store version to the queue by DFS.
       */
      Stack<FileStatus> stack = new Stack<>();
      stack.push(fileStatus);
      while (!stack.isEmpty()) {
        FileStatus currentNameNode = stack.pop();
        FileStatus[] childrenNodes = fs.listStatus(currentNameNode.getPath(), PATH_FILTER);
        /**
         * found .../hourly/year/month/day/hour/ directory. Under this directory is a list of avro files.
         */
        if (currentNameNode.isDirectory() && childrenNodes.length != 0 && childrenNodes[0].isFile()) {
          snapshotInfo.addFileForStoreVersion(version, currentNameNode.getPath());
        } else {
          for (FileStatus childNameNode : childrenNodes) {
            stack.push(childNameNode);
          }
        }
      }
    }
    return snapshotInfoMap;
  }

  /**
   * Helper function that builds the map from etl store name to user proxy name.
   */
  public static void setUpETLStoreToUserName(VeniceProperties props, Map<String, String> ETLStoreToUserName) {
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

  protected static void waitForDistcpJobs(Map<Pair<String, String>, JobExecutionDriver> distcpJobFutures) throws Exception {
    /**
     * Wait for each jobFuture to return. It keeps looping the jobs until all of them have finished.
     */
    while(!distcpJobFutures.entrySet().isEmpty()) {
      Iterator<Map.Entry<Pair<String, String>, JobExecutionDriver>> itr = distcpJobFutures.entrySet().iterator();
      while (itr.hasNext()) {
        Map.Entry<Pair<String, String>, JobExecutionDriver> jobEntry = itr.next();
        String storeName = jobEntry.getKey().getFirst();
        String destination = jobEntry.getKey().getSecond();
        JobState.RunningState jobState = jobEntry.getValue().getJobExecutionState().getRunningState();
        if (jobState.isSuccess()) {
          logger.info("Successfully published the latest delta/snapshot for store " + storeName + " in " + destination);
          itr.remove();
        } else if (jobState.isFailure()) {
          logger.info("Distcp job failed for store " + storeName, jobEntry.getValue().get().getErrorCause());
          itr.remove();
        } else if (jobState.isCancelled()) {
          logger.info("Distcp job canceled for store " + storeName + " in " + destination);
          itr.remove();
        } else {
          //The job is still running or pending, sleep for 1s
          logger.info("Distcp job for store " + storeName + "is still ongoing.");
          TimeUnit.MINUTES.sleep(1);
        }
      }
    }
    distcpJobFutures.clear();
  }

  protected static void checkAllExecutorsFinish(Map<String, ExecutorService> storeNameToExecutors) {
    Iterator<Map.Entry<String, ExecutorService>> itr = storeNameToExecutors.entrySet().iterator();
    while (itr.hasNext()) {
      Map.Entry<String, ExecutorService> executor = itr.next();
      ExecutorService threadPool = executor.getValue();
      threadPool.shutdown();
      itr.remove();
      try {
        boolean taskFinished = threadPool.awaitTermination(5000, TimeUnit.MILLISECONDS);
        if (taskFinished) {
          logger.info("Successfully published all deltas for store " + executor.getKey() +
              " , and the thread pool for the store is shut down.");
        } else {
          logger.warn("Time out publishing for store " + executor.getKey());
        }
      } catch (InterruptedException e) {
        threadPool.shutdownNow();
        logger.warn("Interrupted while waiting for store " + executor.getKey() + " to finish distcp.");
      }
    }
  }

  /**
   * Delete all old snapshots.
   * After calling this method, snapshotQueue will be empty.
   */
  protected static void retireOldFiles(PriorityQueue<Path> snapshotQueue, FileSystem fs, int maxDailySnapshotsToKeep) {
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
   * Delete old snapshots in destination.
   * @param storePath
   * @throws IOException
   */
  protected static void retireDestinationOldFiles(Path storePath, FileSystem fs) throws IOException {
    FileStatus[] fileStatuses = fs.listStatus(storePath, PATH_FILTER);
    PriorityQueue<Path> snapshotQueue = new PriorityQueue<>(new Comparator<Path>() {
      @Override
      public int compare(Path o1, Path o2) {
        return -1 * o1.compareTo(o2);
      }
    });
    for (FileStatus fileStatus: fileStatuses) {
      snapshotQueue.add(fileStatus.getPath());
    }
    retireOldFiles(snapshotQueue, fs, DEFAULT_MAX_FILES_TO_KEEP);
  }

  /**
   * A helper function to check whether the input string follow such
   */
  public static boolean isValidSnapshotPath(String path) {
    Matcher m = SNAPSHOT_PATTERN.matcher(path);
    return m.matches();
  }

  /**
   * A helper function to get record number in a snapshot from its snapshot name
   */
  public static long getSnapshotRecordCount(String snapshotName) {
    if (!isValidSnapshotPath(snapshotName)) {
      return 0;
    }
    int snapshotSeparatorIndex = snapshotName.indexOf(SNAPSHOT_SEPARATOR);
    return Long.valueOf(snapshotName.substring(snapshotSeparatorIndex + SNAPSHOT_SEPARATOR.length()));
  }

  /**
   * Helper function that adds customized configs to Gobblin distcp job.
   */
  protected static void setupConfigs(VeniceGobblinDistcp distcp, VeniceProperties properties) {
    for (String configKey : properties.keySet()) {
      if (configKey.startsWith(DISTCP_CONFIG_PREFIX)) {
        /**
         * The prefix is used to distinguish the distcp configs from VeniceETLPublisher job config,
         * remove the prefix before applying it on distcp
         */
        String conf = configKey.substring(DISTCP_CONFIG_PREFIX.length());
        distcp.setDistcpConfigs(conf, properties.getString(configKey));
        logger.info("Added config " + conf + " with value " + properties.getString(configKey) + " to distcp");
      }
    }
  }
}
