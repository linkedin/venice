package com.linkedin.davinci.blobtransfer;

import static com.linkedin.venice.CommonConfigKeys.SSL_FACTORY_CLASS_NAME;
import static com.linkedin.venice.ConfigKeys.BLOB_TRANSFER_ACL_ENABLED;
import static com.linkedin.venice.ConfigKeys.BLOB_TRANSFER_SSL_ENABLED;
import static com.linkedin.venice.VeniceConstants.DEFAULT_SSL_FACTORY_CLASS_NAME;
import static com.linkedin.venice.store.rocksdb.RocksDBUtils.composePartitionDbDir;
import static org.apache.commons.codec.digest.DigestUtils.md5Hex;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.SSLConfig;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.utils.SslUtils;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.ssl.SslHandler;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class BlobTransferUtils {
  private static final Logger LOGGER = LogManager.getLogger(BlobTransferUtils.class);
  public static final String BLOB_TRANSFER_STATUS = "X-Blob-Transfer-Status";
  public static final String BLOB_TRANSFER_COMPLETED = "Completed";
  public static final String BLOB_TRANSFER_TYPE = "X-Blob-Transfer-Type";

  public enum BlobTransferType {
    FILE, METADATA
  }

  public enum BlobTransferTableFormat {
    PLAIN_TABLE, BLOCK_BASED_TABLE
  }

  public enum BlobTransferStatus {
    /**
     * Transfer is not started yet.
     */
    TRANSFER_NOT_STARTED,

    /**
     * Transfer has been initiated and is in progress.
     * This is the initial state when blob transfer starts.
     */
    TRANSFER_STARTED,

    /**
     * Cancellation has been requested.
     * - Cancellation flag set
     * - Active channel closed
     * - Waiting for transfer future to complete with cancellation exception
     */
    TRANSFER_CANCEL_REQUESTED,

    /**
     * Transfer was successfully cancelled.
     * - Transfer future completed with VeniceBlobTransferCancelledException
     * - Cancellation is confirmed complete
     */
    TRANSFER_CANCELLED,

    /**
     * Transfer completed successfully without cancellation.
     * - Transfer future completed normally
     * - No cancellation was requested
     */
    TRANSFER_COMPLETED
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

  /**
   * Create an SSLFactory from the Venice config loader
   *
   * @param configLoader The Venice config loader containing SSL configuration
   * @return Optional SSLFactory, which will be empty if SSL is not enabled
   */
  public static Optional<SSLFactory> createSSLFactoryForBlobTransferInDVC(VeniceConfigLoader configLoader) {
    // Check if SSL is enabled
    if (!isBlobTransferDVCSslEnabled(configLoader)) {
      throw new IllegalArgumentException("Blob transfer SSL is not enabled");
    }

    try {
      // Create SSL factory
      String sslFactoryClassName =
          configLoader.getCombinedProperties().getString(SSL_FACTORY_CLASS_NAME, DEFAULT_SSL_FACTORY_CLASS_NAME);
      SSLConfig sslConfig = new SSLConfig(configLoader.getCombinedProperties());
      SSLFactory sslFactory = SslUtils.getSSLFactory(sslConfig.getSslProperties(), sslFactoryClassName);
      return Optional.of(sslFactory);
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to create SSL factory for blob transfer", e);
    }
  }

  public static SslHandler createBlobTransferClientSslHandler(Optional<SSLFactory> sslFactory) {
    javax.net.ssl.SSLEngine engine = sslFactory.get().getSSLContext().createSSLEngine();
    engine.setUseClientMode(true);

    SslHandler sslHandler = new SslHandler(engine);
    sslHandler.setHandshakeTimeoutMillis(20000); // 20 seconds for handshake timeout

    return sslHandler;
  }

  /**
   * Create the acl handler for blob transfer, for both DVC peers and server peers
   */
  public static Optional<BlobTransferAclHandler> createAclHandler(VeniceConfigLoader configLoader) {
    if (!isBlobTransferDVCSslEnabled(configLoader) || !isBlobTransferAclValidationEnabled(configLoader)) {
      String errorMsg =
          "Blob transfer SSL or ACL validation is not enabled. sslEnabled: " + isBlobTransferDVCSslEnabled(configLoader)
              + ", aclEnabled: " + isBlobTransferAclValidationEnabled(configLoader) + ", skip create ACL handler.";
      throw new IllegalArgumentException(errorMsg);
    }
    try {
      return Optional.of(new BlobTransferAclHandler());
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to create ACL handler for blob transfer", e);
    }
  }

  private static boolean isBlobTransferDVCSslEnabled(VeniceConfigLoader configLoader) {
    return configLoader.getCombinedProperties().getBoolean(BLOB_TRANSFER_SSL_ENABLED, false);
  }

  private static boolean isBlobTransferAclValidationEnabled(VeniceConfigLoader configLoader) {
    return configLoader.getCombinedProperties().getBoolean(BLOB_TRANSFER_ACL_ENABLED, false);
  }

  /**
   * A config check to determine if blob transfer manager is enabled
   * @param backendConfig the Venice server config
   * @param isIsolatedIngestionEnabled whether isolated ingestion is enabled
   * @return true if blob transfer manager is enabled, false otherwise
   */
  public static boolean isBlobTransferManagerEnabled(
      VeniceServerConfig backendConfig,
      boolean isIsolatedIngestionEnabled) {
    // Blob transfer feature and isolated ingestion feature are mutually exclusive
    if (backendConfig.isBlobTransferManagerEnabled() && backendConfig.isBlobTransferSslEnabled()
        && backendConfig.isBlobTransferAclEnabled()) {
      if (isIsolatedIngestionEnabled) {
        throw new VeniceException("Blob transfer manager is not supported with isolated ingestion");
      } else {
        return true;
      }
    } else if (backendConfig.isBlobTransferManagerEnabled()) {
      throw new VeniceException("Blob transfer manager is not supported without SSL and ACL enabled");
    }
    return false;
  }
}
