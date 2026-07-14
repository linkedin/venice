package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES;
import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYMANAGER_ALGORITHM;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_LOCATION;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_PASSWORD;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEYSTORE_TYPE;
import static com.linkedin.venice.CommonConfigKeys.SSL_KEY_PASSWORD;
import static com.linkedin.venice.CommonConfigKeys.SSL_SECURE_RANDOM_IMPLEMENTATION;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTMANAGER_ALGORITHM;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTSTORE_LOCATION;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTSTORE_PASSWORD;
import static com.linkedin.venice.CommonConfigKeys.SSL_TRUSTSTORE_TYPE;
import static com.linkedin.venice.ConfigKeys.BLOB_TRANSFER_ACL_ENABLED;
import static com.linkedin.venice.ConfigKeys.BLOB_TRANSFER_DISABLED_OFFSET_LAG_THRESHOLD;
import static com.linkedin.venice.ConfigKeys.BLOB_TRANSFER_MANAGER_ENABLED;
import static com.linkedin.venice.ConfigKeys.BLOB_TRANSFER_SSL_ENABLED;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.DAVINCI_BLOB_TRANSFER_SERVER_FALLBACK_ENABLED;
import static com.linkedin.venice.ConfigKeys.DAVINCI_P2P_BLOB_TRANSFER_CLIENT_PORT;
import static com.linkedin.venice.ConfigKeys.DAVINCI_P2P_BLOB_TRANSFER_SERVER_PORT;
import static com.linkedin.venice.ConfigKeys.DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.PUSH_STATUS_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_BLOB_TRANSFER_ACCEPT_CLIENT_REQUEST_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_HTTP2_INBOUND_ENABLED;
import static com.linkedin.venice.integration.utils.DaVinciTestContext.getCachingDaVinciClientFactory;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_KEY_SCHEMA;
import static com.linkedin.venice.meta.PersistenceType.ROCKS_DB;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.SslUtils.LOCAL_KEYSTORE_JKS;
import static com.linkedin.venice.utils.SslUtils.LOCAL_PASSWORD;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithIntToStringSchema;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * End-to-end mechanics test for the Stateful CDC / Da Vinci client falling back to a <b>Venice server</b> for blob
 * transfer when no Da Vinci peer is available. A single blob-serving server hosts the store;
 * a fresh Da Vinci client with the server-fallback flag and no peers should cold-start by pulling the snapshot
 * directly from the server (instead of replaying the Version Topic, which is forced off below).
 *
 * <p><b>Scope / known limitations of this local test:</b>
 * <ul>
 *   <li><b>Origin is SERVER, not CLIENT.</b> The client and server share the same local keystore, so the server's
 *       {@code determineRequestOrigin} (application-identity equality) classifies the client's request as
 *       SERVER-origin. This validates the <i>fallback discovery + transfer mechanics</i>, but NOT the client-origin
 *       gating (accept-flag, store ACL, client cap) — those are covered by unit tests and need distinct cert
 *       identities to exercise end-to-end.</li>
 *   <li><b>Single server, swapped p2p ports.</b> Blob transfer assumes a fleet-wide fixed p2p port; locally we fake
 *       it by setting the client's {@code clientPort} to the server's blob {@code serverPort} so the client connects
 *       to the server's blob endpoint.</li>
 *   <li><b>Host-format probe.</b> The fallback feeds the server hosts from {@code MetadataResponseRecord.getRoutingInfo()}
 *       into the same {@code getConnectableHosts} path the peer flow uses. This test is where a mismatch between the
 *       metadata replica-host format and what the blob-transfer connect expects would surface.</li>
 * </ul>
 */
public class DaVinciClientServerFallbackBlobTransferTest {
  private static final int TEST_TIMEOUT = 180_000;
  // Dedicated bound for the cold-start subscribe and the follow-up read. Kept well under the method-level
  // TEST_TIMEOUT so a hung blob-transfer / subscription / read path fails promptly with a clear TimeoutException,
  // instead of stalling until the suite-level timeout.
  private static final int CLIENT_BOOTSTRAP_TIMEOUT_SECONDS = 60;

  private VeniceClusterWrapper cluster;
  private D2Client d2Client;
  // The blob p2p port the single server serves on; the client's blob clientPort is set to this so it connects here.
  private int serverBlobServerPort;
  private int serverBlobClientPort;

  @BeforeClass
  public void setUp() {
    Utils.thisIsLocalhost();
    serverBlobServerPort = TestUtils.getFreePort();
    serverBlobClientPort = TestUtils.getFreePort();
    while (serverBlobClientPort == serverBlobServerPort) {
      serverBlobClientPort = TestUtils.getFreePort();
    }

    Properties clusterConfig = new Properties();
    clusterConfig.put(PUSH_STATUS_STORE_ENABLED, true);
    clusterConfig.put(DAVINCI_PUSH_STATUS_SCAN_INTERVAL_IN_SECONDS, 3);
    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .numberOfServers(0)
        .numberOfRouters(1)
        .replicationFactor(1)
        .sslToStorageNodes(true)
        .sslToKafka(false)
        .extraProperties(clusterConfig)
        .build();
    cluster = ServiceFactory.getVeniceCluster(options);

    // Add a single server configured to serve blobs. This full-cluster test uses the shared cluster SSL identity, so it
    // exercises server fallback mechanics; CLIENT-origin gating is covered by TestClientOriginServerBlobTransfer.
    Properties serverProperties = new Properties();
    serverProperties.put(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB);
    serverProperties.setProperty(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "false");
    serverProperties.setProperty(DATA_BASE_PATH, Utils.getTempDataDirectory().getAbsolutePath());
    serverProperties.setProperty(DAVINCI_P2P_BLOB_TRANSFER_SERVER_PORT, String.valueOf(serverBlobServerPort));
    serverProperties.setProperty(DAVINCI_P2P_BLOB_TRANSFER_CLIENT_PORT, String.valueOf(serverBlobClientPort));
    serverProperties.setProperty(BLOB_TRANSFER_MANAGER_ENABLED, "true");
    serverProperties.setProperty(BLOB_TRANSFER_SSL_ENABLED, "true");
    serverProperties.setProperty(BLOB_TRANSFER_ACL_ENABLED, "true");
    serverProperties.setProperty(SERVER_BLOB_TRANSFER_ACCEPT_CLIENT_REQUEST_ENABLED, "false");
    // The server's metadata endpoint is reached over D2; enabling HTTP/2 inbound makes the server's D2 service
    // prioritize the https scheme (see VeniceServerWrapper), so the HTTPS d2 client connects over TLS instead of
    // hitting the SSL-only port in plaintext (which returns 403). Mirrors the Fast Client request-based-metadata tests.
    serverProperties.setProperty(SERVER_HTTP2_INBOUND_ENABLED, "true");
    serverProperties.setProperty(BLOB_TRANSFER_DISABLED_OFFSET_LAG_THRESHOLD, "-1000000");

    Properties serverFeatureProperties = new Properties();
    serverFeatureProperties.put(VeniceServerWrapper.SERVER_ENABLE_SSL, "true");
    cluster.addVeniceServer(serverFeatureProperties, serverProperties);

    // The server's metadata endpoint (used by server blob discovery) is SSL-only, so the D2 client the finder reuses
    // must be HTTPS-enabled — same as Fast Client's RequestBasedMetadata reads from servers. (The peer path's
    // DaVinciBlobFinder hits the Router's blob_discovery endpoint, which does not gate SSL, so it tolerated plaintext.)
    d2Client = D2TestUtils.getAndStartHttpsD2Client(cluster.getZk().getAddress());
  }

  @AfterClass
  public void cleanUp() {
    if (d2Client != null) {
      D2ClientUtils.shutdownClient(d2Client);
    }
    Utils.closeQuietlyWithErrorLogged(cluster);
  }

  /**
   * Verifies that a Stateful CDC / Da Vinci client with no peers cold-starts by pulling the snapshot directly from a
   * <b>Venice server</b> rather than replaying the Version Topic. The assertion requires a positive blob-transfer
   * metric, so it cannot false-pass via VT replay.
   *
   * <p>Server discovery hits the server-served {@code metadata/<store>} endpoint, which is SSL-only and gated on the
   * store's storage-node read quota. The harness therefore uses an HTTPS D2 client, sets
   * {@code SERVER_HTTP2_INBOUND_ENABLED} so the server's D2 service advertises https, and enables
   * {@code setStorageNodeReadQuotaEnabled(true)} on the store.
   */
  @Test(timeOut = TEST_TIMEOUT, enabled = true)
  public void testClientColdStartsFromServerWhenNoPeers() throws Exception {
    String storeName = Utils.getUniqueString("server-fallback-store");
    setUpStore(storeName);

    String keyStorePath = SslUtils.getPathForResource(LOCAL_KEYSTORE_JKS);
    String dvcPath = Utils.getTempDataDirectory().getAbsolutePath();

    // Consuming client: enable blob transfer + the server-fallback flag, force blob transfer over VT, and match the
    // blob clientPort to the server's blob serverPort so the client connects to the server's blob endpoint. No peer
    // Da Vinci client is started, so peer discovery returns empty and the client must fall back to the server.
    VeniceProperties backendConfig = new PropertyBuilder().put(PERSISTENCE_TYPE, ROCKS_DB)
        .put(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "false")
        .put(ROCKSDB_BLOCK_CACHE_SIZE_IN_BYTES, 2 * 1024 * 1024L)
        .put(DATA_BASE_PATH, dvcPath)
        .put(PUSH_STATUS_STORE_ENABLED, true)
        .put(BLOB_TRANSFER_MANAGER_ENABLED, true)
        .put(BLOB_TRANSFER_SSL_ENABLED, true)
        .put(BLOB_TRANSFER_ACL_ENABLED, true)
        .put(DAVINCI_BLOB_TRANSFER_SERVER_FALLBACK_ENABLED, true)
        .put(BLOB_TRANSFER_DISABLED_OFFSET_LAG_THRESHOLD, -1000000)
        .put(DAVINCI_P2P_BLOB_TRANSFER_SERVER_PORT, serverBlobClientPort)
        .put(DAVINCI_P2P_BLOB_TRANSFER_CLIENT_PORT, serverBlobServerPort)
        .put(SSL_KEYSTORE_TYPE, "JKS")
        .put(SSL_KEYSTORE_LOCATION, keyStorePath)
        .put(SSL_KEYSTORE_PASSWORD, LOCAL_PASSWORD)
        .put(SSL_TRUSTSTORE_TYPE, "JKS")
        .put(SSL_TRUSTSTORE_LOCATION, keyStorePath)
        .put(SSL_TRUSTSTORE_PASSWORD, LOCAL_PASSWORD)
        .put(SSL_KEY_PASSWORD, LOCAL_PASSWORD)
        .put(SSL_KEYMANAGER_ALGORITHM, "SunX509")
        .put(SSL_TRUSTMANAGER_ALGORITHM, "SunX509")
        .put(SSL_SECURE_RANDOM_IMPLEMENTATION, "SHA1PRNG")
        .build();

    // Non-isolated so blob transfer + its metrics run in this JVM, where the metric assertion below can observe them.
    DaVinciConfig dvcConfig = new DaVinciConfig().setIsolated(false);
    MetricsRepository metricsRepository = new MetricsRepository();
    try (CachingDaVinciClientFactory factory = getCachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        metricsRepository,
        backendConfig,
        cluster)) {
      DaVinciClient<Integer, Object> client = factory.getAndStartGenericAvroClient(storeName, dvcConfig);
      // If the server fallback works, this completes by bootstrapping the snapshot from the server (VT is forced off).
      client.subscribeAll().get(CLIENT_BOOTSTRAP_TIMEOUT_SECONDS, TimeUnit.SECONDS);

      // The store was pushed with int -> string records keyed 1..N (writeSimpleAvroFileWithIntToStringSchema).
      Object value = client.get(1).get(CLIENT_BOOTSTRAP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      Assert.assertNotNull(value, "Client should have bootstrapped the value for key 1 from the server snapshot");

      // Prove the snapshot came from a server blob transfer (no DVC peers exist) rather than Version Topic replay:
      // a positive blob-transfer time/throughput/received metric must have been recorded (without this the test can
      // false-pass via VT replay). These metrics surface as AsyncGauges that sample asynchronously, so an immediate
      // read can return a stale 0 before the first refresh; poll until the recorded value becomes visible.
      TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, () -> {
        boolean blobTransferOccurred = metricsRepository.metrics().entrySet().stream().anyMatch(metric -> {
          String name = metric.getKey().toLowerCase();
          return name.contains("blob_transfer")
              && (name.contains("received") || name.contains("throughput") || name.contains("time"))
              && metric.getValue().value() > 0d;
        });
        Assert.assertTrue(
            blobTransferOccurred,
            "Expected a positive blob-transfer metric (snapshot served by a Venice server), but found none — "
                + "the client may have fallen back to Version Topic replay");
      });
    }
  }

  private void setUpStore(String storeName) {
    File inputDir = getTempDataDirectory();
    try {
      writeSimpleAvroFileWithIntToStringSchema(inputDir);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties vpjProperties = defaultVPJProps(cluster, inputDirPath, storeName);
    // The server metadata endpoint (used by server blob discovery) is gated on storage-node read quota being enabled
    // for the store (ServerReadMetadataRepository: "Fast client is not enabled for store ..."), so enable it here.
    UpdateStoreQueryParams params = new UpdateStoreQueryParams().setPartitionCount(1)
        .setBlobTransferEnabled(true)
        .setStorageNodeReadQuotaEnabled(true);
    try (ControllerClient controllerClient =
        createStoreForJob(cluster, DEFAULT_KEY_SCHEMA, "\"string\"", vpjProperties)) {
      cluster.createMetaSystemStore(storeName);
      cluster.createPushStatusSystemStore(storeName);
      TestUtils.assertCommand(controllerClient.updateStore(storeName, params));
      IntegrationTestPushUtils.runVPJ(vpjProperties);
      cluster.waitVersion(storeName, 1);
    }
  }
}
