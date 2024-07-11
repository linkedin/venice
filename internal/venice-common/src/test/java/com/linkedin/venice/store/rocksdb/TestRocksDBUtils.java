package com.linkedin.venice.store.rocksdb;

import static com.linkedin.venice.store.rocksdb.RocksDBUtils.deletePartitionDir;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
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
  public void teardown() throws IOException {
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

    deletePartitionDir(baseDir.toString(), "storeName", 1, 1);

    // assert directory does not exist
    assertFalse(Files.exists(baseDir.resolve("storeName_v1/storeName_v1_1")));
  }

  @Test
  public void testDeletePartitionDir_multiplePaths() throws IOException {
    // version 5, partition 5 should exist
    // version 5, partition 6 should be deleted
    // tmp path1 "rocksdb/storeName/5/5/file1.txt"
    // tmp path2 "rocksdb/storeName/5/6/file1.txt"

    Files.createDirectories(baseDir.resolve("storeName_v5/storeName_v5_5"));
    Files.createDirectories(baseDir.resolve("storeName_v5/storeName_v5_6"));

    Files.createFile(baseDir.resolve("storeName_v5/storeName_v5_5/file1.txt"));
    Files.createFile(baseDir.resolve("storeName_v5/storeName_v5_6/file2.txt"));

    // assert files exist
    assertTrue(Files.exists(baseDir.resolve("storeName_v5/storeName_v5_5/file1.txt")));
    assertTrue(Files.exists(baseDir.resolve("storeName_v5/storeName_v5_6/file2.txt")));

    deletePartitionDir(baseDir.toString(), "storeName", 5, 6);

    // assert version 5, partition 5 should exist
    assertTrue(Files.exists(baseDir.resolve("storeName_v5/storeName_v5_5")));

    // assert version 5, partition 6 should not exist
    assertFalse(Files.exists(baseDir.resolve("storeName_v5/storeName_v5_6")));
  }

  @Test
  public void testDeletePartitionDir_EmptyFiles() throws IOException {
    Files.createDirectories(baseDir.resolve("storeName_v2/storeName_v2_2"));
    // assert the temp base directory does exist
    assertTrue(Files.exists(baseDir));
    deletePartitionDir(baseDir.toString(), "storeName", 2, 2);
  }
}
