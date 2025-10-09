package com.linkedin.venice.store.rocksdb;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Comparator;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class RocksDBUtils {
  private static final Logger LOGGER = LogManager.getLogger(RocksDBUtils.class);

  /**
   * With level_compaction_dynamic_level_bytes to be false, the stable LSM structure is not guaranteed,
   * so the maximum overhead could be around 2.111 for hybrid stores.
   * Check https://github.com/facebook/rocksdb/wiki/Leveled-Compaction
   */
  public static final double ROCKSDB_OVERHEAD_RATIO_FOR_HYBRID_STORE = 2.11;
  private static final String PARTITION_DB_NAME_SEP = "_";
  private static final String PARTITION_DB_NAME = "%s" + PARTITION_DB_NAME_SEP + "%d"; // store-name_partition_id

  private static final String TEMP_SST_FILE_DIR = ".sst_files";
  private static final String TEMP_RMD_SST_FILE_DIR = ".sst_rmd_files";
  private static final String TEMP_SST_FILE_PREFIX = "sst_file_";
  private static final String TEMP_RMD_SST_FILE_PREFIX = "sst_rmd_file_";
  private static final String TEMP_SNAPSHOT_DIR = ".snapshot_files";
  private static final String TEMP_TRANSFERRED_PARTITION_DIR_PREFIX = "temp_transferred_";

  public static String getPartitionDbName(String storeName, int partitionId) {
    return String.format(PARTITION_DB_NAME, storeName, partitionId);
  }

  public static String parseStoreNameFromPartitionDbName(String partitionDbName) {
    int index = partitionDbName.lastIndexOf(PARTITION_DB_NAME_SEP);
    if (index <= 0) {
      throw new VeniceException("Invalid partition db name: " + partitionDbName);
    }
    return partitionDbName.substring(0, index);
  }

  public static int parsePartitionIdFromPartitionDbName(String partitionDbName) {
    int index = partitionDbName.lastIndexOf(PARTITION_DB_NAME_SEP);
    if (index == -1 || index == partitionDbName.length() - 1) {
      throw new VeniceException("Invalid partition db name: " + partitionDbName);
    }
    return Integer.parseInt(partitionDbName.substring(index + 1));
  }

  public static String composeStoreDbDir(String dbDir, String storeName) {
    return dbDir + "/" + storeName;
  }

  // ex. /db/directory/myStore_v3/myStore_v3_3/
  public static String composePartitionDbDir(String dbDir, String topicName, int partitionId) {
    return dbDir + File.separator + topicName + File.separator + getPartitionDbName(topicName, partitionId);
  }

  // ex. /db/directory/myStore_v3/temp_transferred_myStore_v3_3/
  public static String composeTempPartitionDir(String dbDir, String topicName, int partitionId) {
    return dbDir + File.separator + topicName + File.separator + TEMP_TRANSFERRED_PARTITION_DIR_PREFIX
        + getPartitionDbName(topicName, partitionId);
  }

  public static boolean isTempPartitionDir(String partitionDir) {
    return partitionDir.contains(TEMP_TRANSFERRED_PARTITION_DIR_PREFIX);
  }

  // ex. /db/directory/storeName_v3/storeName_v3_3/.snapshot_files
  public static String composeSnapshotDir(String dbDir, String topicName, int partitionId) {
    return composePartitionDbDir(dbDir, topicName, partitionId) + File.separator + TEMP_SNAPSHOT_DIR;
  }

  public static String composeSnapshotDir(String composePartitionDbDir) {
    return composePartitionDbDir + File.separator + TEMP_SNAPSHOT_DIR;
  }

  public static String composeTempSSTFileDir(String dbDir, String topicName, int partitionId) {
    return composePartitionDbDir(dbDir, topicName, partitionId) + File.separator + TEMP_SST_FILE_DIR;
  }

  public static String composeTempRMDSSTFileDir(String dbDir, String topicName, int partitionId) {
    return composePartitionDbDir(dbDir, topicName, partitionId) + File.separator + TEMP_RMD_SST_FILE_DIR;
  }

  public static String composeTempSSTFileName(int fileNo) {
    return TEMP_SST_FILE_PREFIX + fileNo;
  }

  public static String composeTempRMDSSTFileName(int fileNo) {
    return TEMP_RMD_SST_FILE_PREFIX + fileNo;
  }

  public static boolean isTempSSTFile(String fileName) {
    return fileName.startsWith(TEMP_SST_FILE_PREFIX);
  }

  public static boolean isTempRMDSSTFile(String fileName) {
    return fileName.startsWith(TEMP_RMD_SST_FILE_PREFIX);
  }

  public static int extractTempSSTFileNo(String fileName) {
    if (!isTempSSTFile(fileName)) {
      throw new VeniceException("Temp SST filename should start with prefix: " + TEMP_SST_FILE_PREFIX);
    }
    return Integer.parseInt(fileName.substring(TEMP_SST_FILE_PREFIX.length()));
  }

  public static int extractTempRMDSSTFileNo(String fileName) {
    if (!isTempRMDSSTFile(fileName)) {
      throw new VeniceException("Temp SST filename should start with prefix: " + TEMP_RMD_SST_FILE_PREFIX);
    }
    return Integer.parseInt(fileName.substring(TEMP_RMD_SST_FILE_PREFIX.length()));
  }

  /**
   * Deletes the files associated with the specified partition directory.
   *
   */
  public static void deleteDirectory(String directoryPathStr) {
    Path path = Paths.get(directoryPathStr);
    if (!Files.exists(path)) {
      return;
    }
    try (Stream<Path> walk = Files.walk(path)) {
      // Sort the paths in reverse order so files are deleted before their parent directories
      walk.sorted(Comparator.reverseOrder()).forEach(p -> {
        try {
          Files.delete(p);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      });
    } catch (Exception e) {
      throw new VeniceException(
          String.format("Error occurred while deleting directory: %s. %s", path, e.getMessage()),
          e);
    }
  }

  /**
   * Rename the temporary transferred partition directory to the final partition directory.
   * example of temp partition dir: /db/directory/myStore_v3/temp_transferred_myStore_v3_3/
   * example of final partition dir: /db/directory/myStore_v3/myStore_v3_3/
   *
   *
   * @param dbDir the base directory where the partition directories are located
   * @param topicName the topic name
   * @param partitionId the partition id
   */
  public static void renameTempTransferredPartitionDirToPartitionDir(String dbDir, String topicName, int partitionId) {
    String tempPartitionPathStr = composeTempPartitionDir(dbDir, topicName, partitionId);
    String partitionPathStr = composePartitionDbDir(dbDir, topicName, partitionId);

    Path tempPartitionDir = Paths.get(tempPartitionPathStr);
    Path finalPartitionDir = Paths.get(partitionPathStr);

    // If the partition folder already exists and is not empty, we should not rename the temp folder to it.
    if (Files.exists(finalPartitionDir) && Files.isDirectory(finalPartitionDir)) {
      String[] files = finalPartitionDir.toFile().list();
      if (files != null && files.length > 0) {
        throw new VeniceException(
            "Final partition directory is not empty: " + finalPartitionDir + ", cannot rename temp directory: "
                + tempPartitionDir);
      }
    }

    if (Files.exists(tempPartitionDir)) {
      try {
        Files.move(tempPartitionDir, finalPartitionDir, StandardCopyOption.REPLACE_EXISTING);
      } catch (Exception e) {
        deleteDirectory(tempPartitionPathStr);
        throw new VeniceException("Failed to move temp directory to final directory: " + tempPartitionDir, e);
      }
    }
  }

  /**
   * Cleans up both the partition directory and the temporary transferred directory for a given store, version, and partition.
   * temp directory example: /db/directory/myStore_v3/temp_transferred_myStore_v3_3/
   * partition directory example: /db/directory/myStore_v3/myStore_v3_3/
   * @param storeName
   * @param versionNumber
   * @param partitionId
   * @param basePath
   */
  public static void cleanupBothPartitionDirAndTempTransferredDir(
      String storeName,
      int versionNumber,
      int partitionId,
      String basePath) {
    String kafkaTopic = Version.composeKafkaTopic(storeName, versionNumber);
    String partitionDir = RocksDBUtils.composePartitionDbDir(basePath, kafkaTopic, partitionId);
    String tempPartitionDir = RocksDBUtils.composeTempPartitionDir(basePath, kafkaTopic, partitionId);

    // Clean partition directory
    File partitionDirFile = new File(partitionDir);
    if (partitionDirFile.exists()) {
      try {
        RocksDBUtils.deleteDirectory(partitionDir);
      } catch (Exception e) {
        throw new VeniceException("Cannot clean up partition directory " + partitionDir, e);
      }
    }

    // Clean temp directory
    File tempPartitionDirFile = new File(tempPartitionDir);
    if (tempPartitionDirFile.exists()) {
      try {
        LOGGER.info(
            "Starting cleanup of temp directory: {} for replica {}",
            tempPartitionDir,
            Utils.getReplicaId(kafkaTopic, partitionId));
        RocksDBUtils.deleteDirectory(tempPartitionDir);
        LOGGER.info(
            "Completed cleanup of temp directory: {}, tempPartition existence {}",
            tempPartitionDir,
            tempPartitionDirFile.exists());
      } catch (Exception e) {
        throw new VeniceException("Cannot clean up temp directory " + tempPartitionDir, e);
      }
    }

    if (partitionDirFile.exists()) {
      throw new VeniceException(
          String.format(
              "Partition directory %s still exists after cleanup for %s",
              partitionDir,
              Utils.getReplicaId(kafkaTopic, partitionId)));
    }

    if (tempPartitionDirFile.exists()) {
      throw new VeniceException(
          String.format(
              "Temp partition directory %s still exists after cleanup for %s",
              tempPartitionDir,
              Utils.getReplicaId(kafkaTopic, partitionId)));
    }
  }
}
