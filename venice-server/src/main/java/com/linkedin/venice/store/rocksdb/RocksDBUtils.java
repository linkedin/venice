package com.linkedin.venice.store.rocksdb;

import com.linkedin.venice.exceptions.VeniceException;
import java.io.File;


public class RocksDBUtils {
  private static final String PARTITION_DB_NAME_SEP = "_";
  private static final String PARTITION_DB_NAME = "%s" + PARTITION_DB_NAME_SEP + "%d"; // store-name_partition_id

  private static final String TEMP_SST_FILE_DIR = ".sst_files";
  private static final String TEMP_SST_FILE_PREFIX = "sst_file_";

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
    if (-1 == index || index == partitionDbName.length() - 1) {
      throw new VeniceException("Invalid partition db name: " + partitionDbName);
    }
    return Integer.parseInt(partitionDbName.substring(index + 1));
  }

  public static String composeStoreDbDir(String dbDir, String storeName) {
    return dbDir + "/" + storeName;
  }

  public static String composePartitionDbDir(String dbDir, String storeName, int partitionId) {
    return dbDir + File.separator + storeName + File.separator + getPartitionDbName(storeName, partitionId);
  }

  public static String composeTempSSTFileDir(String dbDir, String storeName, int partitionId) {
    return composePartitionDbDir(dbDir, storeName, partitionId) + File.separator + TEMP_SST_FILE_DIR;
  }

  public static String composeTempSSTFileName(int fileNo) {
    return TEMP_SST_FILE_PREFIX + fileNo;
  }

  public static boolean isTempSSTFile(String fileName) {
    return fileName.startsWith(TEMP_SST_FILE_PREFIX);
  }

  public static int extractTempSSTFileNo(String fileName) {
    if (!isTempSSTFile(fileName)) {
      throw new VeniceException("Temp SST filename should start with prefix: " + TEMP_SST_FILE_PREFIX);
    }
    return Integer.parseInt(fileName.substring(TEMP_SST_FILE_PREFIX.length()));
  }
}