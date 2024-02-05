package com.linkedin.davinci.store.rocksdb;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.validation.checksum.CheckSum;
import com.linkedin.venice.kafka.validation.checksum.CheckSumType;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.io.FileUtils;
import org.rocksdb.EnvOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RocksDBSstFileWriterTest {
  private static final String STORE_NAME = Utils.getUniqueString("sstTest");
  private static final int PARTITION_ID = 0;
  private static final String DB_DIR = Utils.getUniqueTempPath("sstTest");
  private static final boolean IS_RMD = false;
  private static final RocksDBServerConfig ROCKS_DB_SERVER_CONFIG = new RocksDBServerConfig(VeniceProperties.empty());

  @Test
  public void testCheckDatabaseIntegrityWithEmptyCheckpoint() {
    RocksDBSstFileWriter rocksDBSstFileWriter = null;
    try {
      rocksDBSstFileWriter = new RocksDBSstFileWriter(
          STORE_NAME,
          PARTITION_ID,
          "",
          new EnvOptions(),
          new Options(),
          DB_DIR,
          IS_RMD,
          ROCKS_DB_SERVER_CONFIG);
      Map<String, String> checkpointedInfo = new HashMap<>();

      // Expect false as that file is not found
      Assert.assertEquals(rocksDBSstFileWriter.checkDatabaseIntegrity(checkpointedInfo), true);
    } finally {
      if (rocksDBSstFileWriter != null) {
        rocksDBSstFileWriter.close();
      }
    }
  }

  @Test
  public void testCheckDatabaseIntegrityWithValidCheckpoint() throws IOException {
    RocksDBSstFileWriter rocksDBSstFileWriter = null;
    try {
      rocksDBSstFileWriter = new RocksDBSstFileWriter(
          STORE_NAME,
          PARTITION_ID,
          "",
          new EnvOptions(),
          new Options(),
          DB_DIR,
          IS_RMD,
          ROCKS_DB_SERVER_CONFIG);
      Map<String, String> checkpointedInfo = new HashMap<>();

      // Checkpoint that 1 sst file should be found
      checkpointedInfo.put(rocksDBSstFileWriter.getLastCheckPointedSSTFileNum(), "0");
      // create 1 sst file
      createSstFiles(1);

      // Expect true as that file is found
      Assert.assertEquals(rocksDBSstFileWriter.checkDatabaseIntegrity(checkpointedInfo), true);

      // check that all files still remains
      Assert.assertEquals(getNumberOfFilesInTempDirectory(), 1);
    } finally {
      if (rocksDBSstFileWriter != null) {
        rocksDBSstFileWriter.close();
      }
      deleteTempDatabaseDir();
    }
  }

  @Test
  public void testCheckDatabaseIntegrityWithInValidCheckpointV1() throws IOException {
    RocksDBSstFileWriter rocksDBSstFileWriter = null;
    try {
      rocksDBSstFileWriter = new RocksDBSstFileWriter(
          STORE_NAME,
          PARTITION_ID,
          "",
          new EnvOptions(),
          new Options(),
          DB_DIR,
          IS_RMD,
          ROCKS_DB_SERVER_CONFIG);
      Map<String, String> checkpointedInfo = new HashMap<>();

      // Checkpoint that 2 sst file should be found
      checkpointedInfo.put(rocksDBSstFileWriter.getLastCheckPointedSSTFileNum(), "1");
      // Expect false as that file won't be found
      Assert.assertEquals(rocksDBSstFileWriter.checkDatabaseIntegrity(checkpointedInfo), false);
    } finally {
      if (rocksDBSstFileWriter != null) {
        rocksDBSstFileWriter.close();
      }
      deleteTempDatabaseDir();
    }
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*shouldn't be negative")
  public void testCheckDatabaseIntegrityWithInValidCheckpointV2() throws IOException {
    RocksDBSstFileWriter rocksDBSstFileWriter = null;
    try {
      rocksDBSstFileWriter = new RocksDBSstFileWriter(
          STORE_NAME,
          PARTITION_ID,
          "",
          new EnvOptions(),
          new Options(),
          DB_DIR,
          IS_RMD,
          ROCKS_DB_SERVER_CONFIG);
      Map<String, String> checkpointedInfo = new HashMap<>();

      // Checkpoint -1: invalid
      checkpointedInfo.put(rocksDBSstFileWriter.getLastCheckPointedSSTFileNum(), "-1");
      rocksDBSstFileWriter.checkDatabaseIntegrity(checkpointedInfo);
    } finally {
      if (rocksDBSstFileWriter != null) {
        rocksDBSstFileWriter.close();
      }
      deleteTempDatabaseDir();
    }
  }

  @Test
  public void testCheckDatabaseIntegrityWithCheckpointLessThanNumberOfFiles() throws IOException {
    RocksDBSstFileWriter rocksDBSstFileWriter = null;
    try {
      rocksDBSstFileWriter = new RocksDBSstFileWriter(
          STORE_NAME,
          PARTITION_ID,
          "",
          new EnvOptions(),
          new Options(),
          DB_DIR,
          IS_RMD,
          ROCKS_DB_SERVER_CONFIG);
      Map<String, String> checkpointedInfo = new HashMap<>();

      // Checkpoint that 6 sst file should be found
      checkpointedInfo.put(rocksDBSstFileWriter.getLastCheckPointedSSTFileNum(), "5");
      // create 10 sst file
      createSstFiles(10);

      // Expect true as all required files are found and more!
      Assert.assertEquals(rocksDBSstFileWriter.checkDatabaseIntegrity(checkpointedInfo), true);

      // check that only 6 files exists (0-5): Remaining file will be deleted
      Assert.assertEquals(getNumberOfFilesInTempDirectory(), 6);
    } finally {
      if (rocksDBSstFileWriter != null) {
        rocksDBSstFileWriter.close();
      }
      deleteTempDatabaseDir();
    }
  }

  @Test
  public void testCheckDatabaseIntegrityWithMissingFile() throws IOException {
    RocksDBSstFileWriter rocksDBSstFileWriter = null;
    try {
      rocksDBSstFileWriter = new RocksDBSstFileWriter(
          STORE_NAME,
          PARTITION_ID,
          "",
          new EnvOptions(),
          new Options(),
          DB_DIR,
          IS_RMD,
          ROCKS_DB_SERVER_CONFIG);
      Map<String, String> checkpointedInfo = new HashMap<>();

      // Checkpoint that 6 sst file should be found
      checkpointedInfo.put(rocksDBSstFileWriter.getLastCheckPointedSSTFileNum(), "5");
      // create 10 sst file
      createSstFiles(10);
      // delete one of the required file: eg: 1
      deleteSstFile(1);

      // Expect false as all required files are not found
      Assert.assertEquals(rocksDBSstFileWriter.checkDatabaseIntegrity(checkpointedInfo), false);

      // check that no files exists: All files will be deleted
      Assert.assertEquals(getNumberOfFilesInTempDirectory(), 0);
    } finally {
      if (rocksDBSstFileWriter != null) {
        rocksDBSstFileWriter.close();
      }
      deleteTempDatabaseDir();
    }
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Checkpointed info and SST files in.*doesn't match.*")
  public void testOpenWithMissingFile() throws IOException {
    RocksDBSstFileWriter rocksDBSstFileWriter = null;
    try {
      rocksDBSstFileWriter = new RocksDBSstFileWriter(
          STORE_NAME,
          PARTITION_ID,
          "",
          new EnvOptions(),
          new Options(),
          DB_DIR,
          IS_RMD,
          ROCKS_DB_SERVER_CONFIG);
      Map<String, String> checkpointedInfo = new HashMap<>();

      // Checkpoint that 6 sst file should be found
      checkpointedInfo.put(rocksDBSstFileWriter.getLastCheckPointedSSTFileNum(), "5");
      // create 10 sst file
      createSstFiles(10);
      // delete one of the required file: eg: 1
      deleteSstFile(1);

      rocksDBSstFileWriter.open(checkpointedInfo, Optional.empty());
    } finally {
      if (rocksDBSstFileWriter != null) {
        rocksDBSstFileWriter.close();
      }
      deleteTempDatabaseDir();
    }
  }

  @Test
  public void testOpenAndSyncWithAllValidFiles() throws IOException {
    RocksDBSstFileWriter rocksDBSstFileWriter = null;
    try {
      rocksDBSstFileWriter = new RocksDBSstFileWriter(
          STORE_NAME,
          PARTITION_ID,
          "",
          new EnvOptions(),
          new Options(),
          DB_DIR,
          IS_RMD,
          ROCKS_DB_SERVER_CONFIG);
      Map<String, String> checkpointedInfo = new HashMap<>();

      // Checkpoint that 6 sst file should be found
      checkpointedInfo.put(rocksDBSstFileWriter.getLastCheckPointedSSTFileNum(), "5");
      // create 10 sst file
      createSstFiles(10);

      rocksDBSstFileWriter.open(checkpointedInfo, Optional.empty());
      // check that only 7 files exists (0-5 after cleanup by checkDatabaseIntegrity)
      // and 6 after rocksDBSstFileWriter.open opens a new file
      Assert.assertEquals(getNumberOfFilesInTempDirectory(), 7);
      rocksDBSstFileWriter.sync();
    } finally {
      if (rocksDBSstFileWriter != null) {
        rocksDBSstFileWriter.close();
      }
      deleteTempDatabaseDir();
    }
  }

  @Test
  public void testSyncWithCorrectChecksum() throws IOException, RocksDBException {
    RocksDBSstFileWriter rocksDBSstFileWriter = null;
    try {
      rocksDBSstFileWriter = new RocksDBSstFileWriter(
          STORE_NAME,
          PARTITION_ID,
          "",
          new EnvOptions(),
          new Options(),
          DB_DIR,
          IS_RMD,
          ROCKS_DB_SERVER_CONFIG);
      Map<String, String> checkpointedInfo = new HashMap<>();

      // Checkpoint that 1 sst file should be found
      checkpointedInfo.put(rocksDBSstFileWriter.getLastCheckPointedSSTFileNum(), "0");
      // create 5 sst file
      createSstFiles(5);

      rocksDBSstFileWriter.open(checkpointedInfo, Optional.of(() -> {
        CheckSum sstFileFinalCheckSum = CheckSum.getInstance(CheckSumType.MD5);
        sstFileFinalCheckSum.update("key".getBytes());
        sstFileFinalCheckSum.update("value".getBytes());
        return sstFileFinalCheckSum.getCheckSum();
      }));
      // check that only 1 files exists ("0" after cleanup by checkDatabaseIntegrity)
      // and "1" after rocksDBSstFileWriter.open opens a new file
      Assert.assertEquals(getNumberOfFilesInTempDirectory(), 2);
      rocksDBSstFileWriter.put("key".getBytes(), ByteBuffer.wrap("value".getBytes()));
      // call sync to verify checksum
      rocksDBSstFileWriter.sync();
    } finally {
      if (rocksDBSstFileWriter != null) {
        rocksDBSstFileWriter.close();
      }
      deleteTempDatabaseDir();
    }
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "verifyChecksum: failure. last sstFile checksum didn't match for store.*")
  public void testSyncWithInCorrectChecksum() throws IOException, RocksDBException {
    RocksDBSstFileWriter rocksDBSstFileWriter = null;
    try {
      rocksDBSstFileWriter = new RocksDBSstFileWriter(
          STORE_NAME,
          PARTITION_ID,
          "",
          new EnvOptions(),
          new Options(),
          DB_DIR,
          IS_RMD,
          ROCKS_DB_SERVER_CONFIG);
      Map<String, String> checkpointedInfo = new HashMap<>();

      // Checkpoint that 1 sst file should be found
      checkpointedInfo.put(rocksDBSstFileWriter.getLastCheckPointedSSTFileNum(), "0");
      // create 10 sst file
      createSstFiles(5);

      rocksDBSstFileWriter.open(checkpointedInfo, Optional.of(() -> {
        CheckSum sstFileFinalCheckSum = CheckSum.getInstance(CheckSumType.MD5);
        sstFileFinalCheckSum.update("wrong_key".getBytes());
        sstFileFinalCheckSum.update("wrong_value".getBytes());
        return sstFileFinalCheckSum.getCheckSum();
      }));
      // check that only 2 files exists ("0" after cleanup by checkDatabaseIntegrity)
      // and "1" after rocksDBSstFileWriter.open opens a new file
      Assert.assertEquals(getNumberOfFilesInTempDirectory(), 2);
      rocksDBSstFileWriter.put("key".getBytes(), ByteBuffer.wrap("value".getBytes()));
      // call sync to verify checksum
      rocksDBSstFileWriter.sync();
    } finally {
      if (rocksDBSstFileWriter != null) {
        rocksDBSstFileWriter.close();
      }
      deleteTempDatabaseDir();
    }
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Checksum mismatch in SST files.*")
  public void testSyncWithMissingFile() throws IOException, RocksDBException {
    RocksDBSstFileWriter rocksDBSstFileWriter = null;
    try {
      rocksDBSstFileWriter = new RocksDBSstFileWriter(
          STORE_NAME,
          PARTITION_ID,
          "",
          new EnvOptions(),
          new Options(),
          DB_DIR,
          IS_RMD,
          ROCKS_DB_SERVER_CONFIG);
      Map<String, String> checkpointedInfo = new HashMap<>();

      // Checkpoint that 6 sst file should be found
      checkpointedInfo.put(rocksDBSstFileWriter.getLastCheckPointedSSTFileNum(), "5");
      // create 10 sst file
      createSstFiles(10);

      rocksDBSstFileWriter.open(checkpointedInfo, Optional.of(() -> "1".getBytes()));
      // check that only 7 files exists (0-5 after cleanup by checkDatabaseIntegrity)
      // and 6 after rocksDBSstFileWriter.open opens a new file
      Assert.assertEquals(getNumberOfFilesInTempDirectory(), 7);
      rocksDBSstFileWriter.put("key".getBytes(), ByteBuffer.wrap("value".getBytes()));
      deleteAllSstFiles(getNumberOfFilesInTempDirectory());
      // call sync after deleting all the files to mimic exception thrown during graceful shutdown of servers with
      // missing sst files
      rocksDBSstFileWriter.sync();
    } finally {
      if (rocksDBSstFileWriter != null) {
        rocksDBSstFileWriter.close();
      }
      deleteTempDatabaseDir();
    }
  }

  private String getTempDatabaseDir() {
    File storeDir = new File(DB_DIR).getAbsoluteFile();
    if (!storeDir.mkdirs()) {
      throw new VeniceException("Failed to mkdirs for path: " + storeDir.getPath());
    }
    storeDir.deleteOnExit();
    return storeDir.getPath();
  }

  private void deleteTempDatabaseDir() throws IOException {
    File directory = new File(DB_DIR);

    // Delete all files in the directory
    FileUtils.cleanDirectory(directory);

    // Delete the directory
    directory.delete();
  }

  private void deleteSstFile(int fileNumber) throws IOException {
    FileUtils.delete(new File(DB_DIR + "/sst_file_" + fileNumber));
  }

  private void deleteAllSstFiles(int maxFileNumber) throws IOException {
    for (int i = 0; i < maxFileNumber; i++) {
      FileUtils.delete(new File(DB_DIR + "/sst_file_" + i));
    }
  }

  private void createSstFiles(int numberOfFiles) throws IOException {
    getTempDatabaseDir();
    for (int i = 0; i < numberOfFiles; i++) {
      new File(DB_DIR + "/sst_file_" + i).createNewFile();
    }
  }

  private int getNumberOfFilesInTempDirectory() {
    File directory = new File(DB_DIR);
    int fileCount = 0;
    File[] listFiles = directory.listFiles();
    assert (listFiles != null);
    if (listFiles.length == 0) {
      return fileCount;
    }
    for (File file: listFiles) {
      if (file != null && file.isFile()) {
        fileCount++;
      }
    }
    return fileCount;
  }
}
