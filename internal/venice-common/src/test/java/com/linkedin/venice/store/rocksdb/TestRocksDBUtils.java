package com.linkedin.venice.store.rocksdb;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.utils.DataProviderUtils;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestRocksDBUtils {
  private Path baseDir;

  @BeforeMethod
  public void setUp() throws IOException {
    baseDir = Files.createTempDirectory("rocksdb");
  }

  @AfterMethod
  public void tearDown() throws IOException {
    if (baseDir != null && Files.exists(baseDir)) {
      Files.walk(baseDir).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);

      // assert the temp base directory does not exist
      assertFalse(Files.exists(baseDir));
    }
  }

  @Test
  public void testDeletePartitionDir() throws IOException {
    Files.createDirectories(baseDir.resolve("storeName_v1/storeName_v1_1"));

    Files.createFile(baseDir.resolve("storeName_v1/storeName_v1_1/file1.txt"));
    Files.createFile(baseDir.resolve("storeName_v1/storeName_v1_1/file2.txt"));

    // assert files exist
    assertTrue(Files.exists(baseDir.resolve("storeName_v1/storeName_v1_1/file1.txt")));
    assertTrue(Files.exists(baseDir.resolve("storeName_v1/storeName_v1_1/file2.txt")));
    String deletePath = RocksDBUtils.composePartitionDbDir(baseDir.toString(), "storeName_v1", 1);
    RocksDBUtils.deleteDirectory(deletePath);

    // assert directory does not exist
    assertFalse(Files.exists(baseDir.resolve("storeName_v1/storeName_v1_1")));
  }

  @Test
  public void testDeletePartitionDir_EmptyFiles() throws IOException {
    Files.createDirectories(baseDir.resolve("storeName_v2/storeName_v2_2"));
    // assert the temp base directory does exist
    assertTrue(Files.exists(baseDir.resolve("storeName_v2/storeName_v2_2")));

    String deletePath = RocksDBUtils.composePartitionDbDir(baseDir.toString(), "storeName_v2", 2);
    RocksDBUtils.deleteDirectory(deletePath);

    // assert the temp base directory does not exist
    assertFalse(Files.exists(baseDir.resolve("storeName_v2/storeName_v2_2")));
  }

  @Test
  public void testRenameTempTransferredPartitionDirToPartitionDir() throws IOException {
    Path tempDir = baseDir.resolve("storeName_v1/temp_transferred_storeName_v1_1");
    Files.createDirectories(tempDir);
    Files.createFile(tempDir.resolve("file1.sst"));
    Files.createFile(tempDir.resolve("MANIFEST-000001"));
    Files.createFile(tempDir.resolve("CURRENT"));

    // Verify temp directory and files exist
    assertTrue(Files.exists(tempDir));
    assertTrue(Files.exists(tempDir.resolve("file1.sst")));
    assertTrue(Files.exists(tempDir.resolve("MANIFEST-000001")));
    assertTrue(Files.exists(tempDir.resolve("CURRENT")));

    // Execute
    RocksDBUtils.renameTempTransferredPartitionDirToPartitionDir(baseDir.toString(), "storeName_v1", 1);

    // Verify temp directory no longer exists
    assertFalse(Files.exists(tempDir));

    // Verify final directory exists with all files
    Path finalDir = baseDir.resolve("storeName_v1/storeName_v1_1");
    assertTrue(Files.exists(finalDir));
    assertTrue(Files.exists(finalDir.resolve("file1.sst")));
    assertTrue(Files.exists(finalDir.resolve("MANIFEST-000001")));
    assertTrue(Files.exists(finalDir.resolve("CURRENT")));
  }

  @Test
  public void testFailRenameTempTransferredPartitionDirToPartitionDir() throws IOException {
    Path tempDir = baseDir.resolve("storeName_v1/temp_transferred_storeName_v1_1");
    Files.createDirectories(tempDir);
    Files.createFile(tempDir.resolve("file1.sst"));
    Files.createFile(tempDir.resolve("MANIFEST-000001"));
    Files.createFile(tempDir.resolve("CURRENT"));

    // Verify temp directory and files exist
    assertTrue(Files.exists(tempDir));
    assertTrue(Files.exists(tempDir.resolve("file1.sst")));
    assertTrue(Files.exists(tempDir.resolve("MANIFEST-000001")));
    assertTrue(Files.exists(tempDir.resolve("CURRENT")));

    // partition dir already exists and is not empty to make rename fail
    Path partitionDir = baseDir.resolve("storeName_v1/storeName_v1_1");
    Files.createDirectories(partitionDir);
    Files.createFile(partitionDir.resolve("existing_file.sst"));

    // Execute
    try {
      RocksDBUtils.renameTempTransferredPartitionDirToPartitionDir(baseDir.toString(), "storeName_v1", 1);
    } catch (VeniceException e) {
      Assert.assertTrue(
          e.getMessage().contains("Final partition directory is not empty"),
          "Expected exception message to contain 'Final partition directory is not empty'");
      // Folders are both existed and failed to rename
      assertTrue(Files.exists(tempDir));
      assertTrue(Files.exists(partitionDir));
    }
  }

  @Test(dataProvider = "Two-True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testCleanupDirectories_VariousExistenceScenarios(boolean partitionExists, boolean tempExists)
      throws IOException {
    String storeName = "testStore";
    int versionNumber = 1;
    int partitionId = 0;
    String kafkaTopic = storeName + "_v" + versionNumber;
    Path partitionDir = baseDir.resolve(kafkaTopic + "/" + kafkaTopic + "_" + partitionId);
    Path tempPartitionDir = baseDir.resolve(kafkaTopic + "/temp_transferred_" + kafkaTopic + "_" + partitionId);

    if (partitionExists) {
      Files.createDirectories(partitionDir);
      Files.createFile(partitionDir.resolve("partition_file.sst"));
    }

    if (tempExists) {
      Files.createDirectories(tempPartitionDir);
      Files.createFile(tempPartitionDir.resolve("temp_file.sst"));
    }

    // Verify initial state
    if (partitionExists) {
      assertTrue(Files.exists(partitionDir));
    }
    if (tempExists) {
      assertTrue(Files.exists(tempPartitionDir));
    }

    // Execute
    RocksDBUtils
        .cleanupBothPartitionDirAndTempTransferredDir(storeName, versionNumber, partitionId, baseDir.toString());

    // Verify both directories are deleted (regardless of initial existence)
    assertFalse(Files.exists(partitionDir));
    assertFalse(Files.exists(tempPartitionDir));
  }
}
