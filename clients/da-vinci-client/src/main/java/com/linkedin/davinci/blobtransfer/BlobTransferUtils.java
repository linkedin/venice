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
import com.linkedin.venice.exceptions.VeniceBlobTransferIncompatibleSchemaException;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.utils.SslUtils;
import io.netty.handler.codec.http.HttpRequest;
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
  /**
   * Protocol version the peer used to serialize the {@code PartitionState} embedded in
   * the metadata response body. The client uses this to fail fast when the local binary
   * does not have a reader for that version, rather than discovering the mismatch during
   * deserialization after the body has been fully received.
   */
  public static final String BLOB_TRANSFER_PARTITION_STATE_SCHEMA_VERSION =
      "X-Blob-Transfer-Partition-State-Schema-Version";
  /** Same purpose as {@link #BLOB_TRANSFER_PARTITION_STATE_SCHEMA_VERSION} but for {@code StoreVersionState}. */
  public static final String BLOB_TRANSFER_STORE_VERSION_STATE_SCHEMA_VERSION =
      "X-Blob-Transfer-Store-Version-State-Schema-Version";
  /**
   * Marker header set on the server's 400 BAD_REQUEST response when it rejects a
   * blob-transfer request because the requester's advertised schema versions do not
   * match the server's local versions. Lets the client distinguish a schema-version
   * rejection from any other 400 without parsing the body.
   */
  public static final String BLOB_TRANSFER_SCHEMA_MISMATCH = "X-Blob-Transfer-Schema-Mismatch";

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
   * Validate that the schema-version headers on a P2P metadata response match the
   * local binary's compiled-in {@code currentProtocolVersion} for each protocol.
   * Throws {@link VeniceBlobTransferIncompatibleSchemaException} when at least one
   * header is present and does not equal the local version — letting the caller fail
   * fast at HTTP header-parse time, before any body is consumed.
   *
   * <p>An exact-match policy is used (rather than e.g. "peer &lt;= local"). Blob
   * transfer is the fast path; if the binaries on the two ends are not in lock-step
   * we want to step aside and let Kafka bootstrap take over rather than rely on
   * cross-version Avro promotion of the partition metadata. Skew between peers is
   * limited to rolling-deploy windows, so the cost of being strict is bounded.
   *
   * <p>Behaviour for the absent / malformed cases is intentionally permissive so a
   * server-side rollout of the new headers cannot break peers that haven't been
   * upgraded yet, and so a header parsing bug cannot crash the channel:
   * <ul>
   *   <li>Both headers absent — pass through (peer is on an older binary).</li>
   *   <li>Header value non-numeric or out of byte range — log a warning and pass
   *       through; the existing deserialization-time exception remains as the safety
   *       net for the truly incompatible case (no regression vs. today).</li>
   * </ul>
   */
  public static void validateMetadataResponseSchemaVersions(HttpResponse response, String peerHost) {
    String psHeader = response.headers().get(BLOB_TRANSFER_PARTITION_STATE_SCHEMA_VERSION);
    String svsHeader = response.headers().get(BLOB_TRANSFER_STORE_VERSION_STATE_SCHEMA_VERSION);
    if (psHeader == null && svsHeader == null) {
      return;
    }

    int peerPs = parseProtocolVersionHeader(psHeader, BLOB_TRANSFER_PARTITION_STATE_SCHEMA_VERSION);
    int peerSvs = parseProtocolVersionHeader(svsHeader, BLOB_TRANSFER_STORE_VERSION_STATE_SCHEMA_VERSION);

    int localPs = AvroProtocolDefinition.PARTITION_STATE.getCurrentProtocolVersion();
    int localSvs = AvroProtocolDefinition.STORE_VERSION_STATE.getCurrentProtocolVersion();

    boolean psMismatch = peerPs != VeniceBlobTransferIncompatibleSchemaException.VERSION_UNKNOWN && peerPs != localPs;
    boolean svsMismatch =
        peerSvs != VeniceBlobTransferIncompatibleSchemaException.VERSION_UNKNOWN && peerSvs != localSvs;

    if (psMismatch || svsMismatch) {
      LOGGER.warn(
          "Aborting P2P blob transfer from peer {}: metadata schema version mismatch"
              + " (peer PartitionState={}, StoreVersionState={}; local PartitionState={}, StoreVersionState={})",
          peerHost,
          renderVersion(peerPs),
          renderVersion(peerSvs),
          localPs,
          localSvs);
      throw new VeniceBlobTransferIncompatibleSchemaException(peerHost, peerPs, peerSvs, localPs, localSvs);
    }
  }

  /**
   * Server-side counterpart of {@link #validateMetadataResponseSchemaVersions}: compare
   * the schema-version headers on a P2P blob-transfer GET request against the local
   * binary's compiled-in {@code currentProtocolVersion}. Used by the server right next
   * to the table-format check so a schema mismatch is rejected with a 400 BAD_REQUEST
   * before any file work begins — otherwise the client would pay for the entire file
   * transfer and only discover the mismatch at the metadata stage.
   *
   * <p>Same equality policy as the response side. Returns a diagnostic string suitable
   * for the response body when the request is incompatible, or {@code null} when it
   * is compatible (or when both headers are absent — older clients that have not yet
   * been upgraded to advertise their versions).
   */
  public static String compareRequestedSchemaVersionsAgainstLocal(HttpRequest request) {
    String psHeader = request.headers().get(BLOB_TRANSFER_PARTITION_STATE_SCHEMA_VERSION);
    String svsHeader = request.headers().get(BLOB_TRANSFER_STORE_VERSION_STATE_SCHEMA_VERSION);
    if (psHeader == null && svsHeader == null) {
      return null;
    }

    int peerPs = parseProtocolVersionHeader(psHeader, BLOB_TRANSFER_PARTITION_STATE_SCHEMA_VERSION);
    int peerSvs = parseProtocolVersionHeader(svsHeader, BLOB_TRANSFER_STORE_VERSION_STATE_SCHEMA_VERSION);

    int localPs = AvroProtocolDefinition.PARTITION_STATE.getCurrentProtocolVersion();
    int localSvs = AvroProtocolDefinition.STORE_VERSION_STATE.getCurrentProtocolVersion();

    boolean psMismatch = peerPs != VeniceBlobTransferIncompatibleSchemaException.VERSION_UNKNOWN && peerPs != localPs;
    boolean svsMismatch =
        peerSvs != VeniceBlobTransferIncompatibleSchemaException.VERSION_UNKNOWN && peerSvs != localSvs;

    if (psMismatch || svsMismatch) {
      return "Blob transfer schema version mismatch: requester PartitionState=" + renderVersion(peerPs)
          + ", StoreVersionState=" + renderVersion(peerSvs) + "; local PartitionState=" + localPs
          + ", StoreVersionState=" + localSvs;
    }
    return null;
  }

  // The header value is peer-controlled, so this can be hit on every request/response if a
  // misbehaving peer keeps sending bad headers. Log at DEBUG to avoid log spam — when this
  // returns VERSION_UNKNOWN the caller treats it as pass-through, and a real version mismatch
  // gets logged at WARN by the caller with full peer-host context. Malformed values that slip
  // through are caught by the existing deserialization-time exception.
  private static int parseProtocolVersionHeader(String value, String headerName) {
    if (value == null) {
      return VeniceBlobTransferIncompatibleSchemaException.VERSION_UNKNOWN;
    }
    try {
      int parsed = Integer.parseInt(value.trim());
      // Protocol versions are encoded into a single byte on the wire (see InternalAvroSpecificSerializer).
      if (parsed < 0 || parsed > Byte.MAX_VALUE) {
        LOGGER.debug("Out-of-range value '{}' for blob-transfer header {}; treating as unknown.", value, headerName);
        return VeniceBlobTransferIncompatibleSchemaException.VERSION_UNKNOWN;
      }
      return parsed;
    } catch (NumberFormatException e) {
      LOGGER.debug("Malformed value '{}' for blob-transfer header {}; treating as unknown.", value, headerName);
      return VeniceBlobTransferIncompatibleSchemaException.VERSION_UNKNOWN;
    }
  }

  private static String renderVersion(int v) {
    return v == VeniceBlobTransferIncompatibleSchemaException.VERSION_UNKNOWN ? "<unknown>" : Integer.toString(v);
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
   * @return true if blob transfer manager is enabled, false otherwise
   */
  public static boolean isBlobTransferManagerEnabled(VeniceServerConfig backendConfig) {
    if (backendConfig.isBlobTransferManagerEnabled() && backendConfig.isBlobTransferSslEnabled()
        && backendConfig.isBlobTransferAclEnabled()) {
      return true;
    } else if (backendConfig.isBlobTransferManagerEnabled()) {
      throw new VeniceException("Blob transfer manager is not supported without SSL and ACL enabled");
    }
    return false;
  }
}
