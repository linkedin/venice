package com.linkedin.venice.store.rocksdb;

import static com.linkedin.venice.store.rocksdb.RocksDBUtils.deleteBlobFiles;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import org.testng.annotations.Test;


public class TestRocksDBUtils {
  @Test
  public void testDeleteBlobFiles() throws IOException {
    // tmp directory "rocksdb/storeName/1/1/{file1.txt|file2.txt}"
    Path baseDir = Files.createTempDirectory("rocksdb");
    Path storeDir = Files.createDirectory(baseDir.resolve("storeName"));
    Path versionDir = Files.createDirectory(storeDir.resolve("1"));
    Path partitionDir = Files.createDirectory(versionDir.resolve("1"));
    Files.createFile(partitionDir.resolve("file1.txt"));
    Files.createFile(partitionDir.resolve("file2.txt"));

    assertTrue(Files.exists(partitionDir.resolve("file1.txt")));
    assertTrue(Files.exists(partitionDir.resolve("file2.txt")));

    // Call the method
    CompletableFuture<Void> future = deleteBlobFiles(baseDir.toString(), "storeName", 1, 1);

    // Wait for the future to complete
    future.join();
    assertTrue(future.isDone());

    // Verify that the directory and files have been deleted
    assertFalse(Files.exists(partitionDir));
  }
}
