package com.linkedin.davinci.blobtransfer;

import static com.linkedin.venice.store.rocksdb.RocksDBUtils.composePartitionDbDir;
import static org.apache.commons.codec.digest.DigestUtils.md5Hex;

import com.linkedin.venice.meta.Version;
import io.netty.handler.codec.http.HttpResponse;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;


public class BlobTransferUtils {
  public static final String BLOB_TRANSFER_STATUS = "X-Blob-Transfer-Status";
  public static final String BLOB_TRANSFER_COMPLETED = "Completed";
  public static final String BLOB_TRANSFER_TYPE = "X-Blob-Transfer-Type";

  public enum BlobTransferType {
    FILE, METADATA
  }

  /**
   * Check if the HttpResponse message is for metadata.
   * @param msg the HttpResponse message
   * @return true if the message is a metadata message, false otherwise
   */
  public static boolean isMetadataMessage(HttpResponse msg) {
    String metadataHeader = msg.headers().get(BlobTransferUtils.BLOB_TRANSFER_TYPE);
    if (metadataHeader == null) {
      return false;
    }
    return metadataHeader.equals(BlobTransferUtils.BlobTransferType.METADATA.name());
  }

  /**
   * Generate MD5 checksum for a file
   * @param filePath the path to the file
   * @return a hex string
   * @throws IOException if an I/O error occurs
   */
  public static String generateFileChecksum(Path filePath) throws IOException {
    String md5Digest;
    try (InputStream inputStream = Files.newInputStream(filePath)) {
      md5Digest = md5Hex(inputStream);
    } catch (IOException e) {
      throw new IOException("Failed to generate checksum for file: " + filePath.toAbsolutePath(), e);
    }
    return md5Digest;
  }

  /**
   * Calculate throughput in MB/sec for a given partition directory
   */
  private static double calculateThroughputInMBPerSec(File partitionDir, double transferTimeInSec) throws IOException {
    if (!partitionDir.exists() || !partitionDir.isDirectory()) {
      throw new IllegalArgumentException(
          "Partition directory does not exist or is not a directory: " + partitionDir.getAbsolutePath());
    }
    // Calculate total size of all files in the directory
    long totalSizeInBytes = getTotalSizeOfFiles(partitionDir);
    // Convert bytes to MB
    double totalSizeInMB = totalSizeInBytes / (1000.0 * 1000.0);
    // Calculate throughput in MB/sec
    double throughput = totalSizeInMB / transferTimeInSec;
    return throughput;
  }

  /**
   * Get total size of all files in a directory
   */
  private static long getTotalSizeOfFiles(File dir) throws IOException {
    return Files.walk(dir.toPath()).filter(Files::isRegularFile).mapToLong(path -> path.toFile().length()).sum();
  }

  /**
   * Calculate throughput per partition in MB/sec
   * @param baseDir the base directory of the underlying storage
   * @param storeName the store name
   * @param version the version of the store
   * @param partition the partition number
   * @param transferTimeInSec the transfer time in seconds
   * @return the throughput in MB/sec
   */
  static double getThroughputPerPartition(
      String baseDir,
      String storeName,
      int version,
      int partition,
      double transferTimeInSec) {
    String topicName = Version.composeKafkaTopic(storeName, version);
    String partitionDir = composePartitionDbDir(baseDir, topicName, partition);
    Path path = null;
    try {
      path = Paths.get(partitionDir);
      File partitionFile = path.toFile();
      return calculateThroughputInMBPerSec(partitionFile, transferTimeInSec);
    } catch (Exception e) {
      return 0;
    }
  }
}
