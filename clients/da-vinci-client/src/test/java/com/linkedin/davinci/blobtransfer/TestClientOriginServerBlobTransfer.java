package com.linkedin.davinci.blobtransfer;

import static com.linkedin.davinci.blobtransfer.BlobTransferGlobalTrafficShapingHandlerHolder.getGlobalChannelTrafficShapingHandlerInstance;
import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.BlobTransferTableFormat;
import static com.linkedin.davinci.blobtransfer.BlobTransferUtils.createAclHandler;
import static com.linkedin.venice.CommonConfigKeys.SSL_ENABLED;
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
import static com.linkedin.venice.utils.TestUtils.DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.davinci.blobtransfer.client.NettyFileTransferClient;
import com.linkedin.davinci.blobtransfer.server.P2PBlobTransferService;
import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.stats.AggBlobTransferStats;
import com.linkedin.davinci.stats.AggVersionedBlobTransferStats;
import com.linkedin.davinci.storage.StorageEngineRepository;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.acl.DynamicAccessController;
import com.linkedin.venice.acl.VeniceComponent;
import com.linkedin.venice.authorization.IdentityParser;
import com.linkedin.venice.blobtransfer.BlobFinder;
import com.linkedin.venice.blobtransfer.BlobPeersDiscoveryResponse;
import com.linkedin.venice.kafka.protocol.state.PartitionState;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.security.DefaultSSLFactory;
import com.linkedin.venice.security.SSLFactory;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.store.rocksdb.RocksDBUtils;
import com.linkedin.venice.utils.LogContext;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.VeniceProperties;
import io.netty.handler.traffic.GlobalChannelTrafficShapingHandler;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Tests the client-origin server blob transfer path over Netty. The test generates same-issuer but different-identity
 * client/server certs so the server classifies the request as CLIENT-origin and exercises the accept flag, store ACL,
 * and client-capacity gates.
 */
public class TestClientOriginServerBlobTransfer {
  private static final String STORE = "test_store";
  private static final int VERSION = 1;
  private static final int PARTITION = 0;
  private static final String KEYSTORE_PASSWORD = "dev_pass";
  private static final int MAX_TRANSFER_TIMEOUT_MIN = 30;
  private static final SecureRandom SECURE_RANDOM = new SecureRandom();

  private Path snapshotDir;
  private Path partitionDir;
  private Path sslDir;
  private Path srcFile;
  private Path destFile;

  private StorageMetadataService storageMetadataService;
  private AggBlobTransferStats blobTransferStats;
  private AggVersionedBlobTransferStats versionedBlobTransferStats;
  private VeniceNotifier notifier;
  private SSLFactory serverSslFactory;
  private SSLFactory clientSslFactory;

  private P2PBlobTransferService server;
  private NettyP2PBlobTransferManager manager;

  @BeforeMethod
  public void setUp() throws Exception {
    snapshotDir = Files.createTempDirectory("clientOriginSnapshot");
    partitionDir = Files.createTempDirectory("clientOriginPartition");

    storageMetadataService = mock(StorageMetadataService.class);
    blobTransferStats = mock(AggBlobTransferStats.class);
    versionedBlobTransferStats = mock(AggVersionedBlobTransferStats.class);
    notifier = mock(VeniceNotifier.class);

    StoreVersionState storeVersionState = new StoreVersionState();
    doReturn(storeVersionState).when(storageMetadataService).getStoreVersionState(any());
    InternalAvroSpecificSerializer<PartitionState> partitionStateSerializer =
        AvroProtocolDefinition.PARTITION_STATE.getSerializer();
    OffsetRecord offsetRecord = new OffsetRecord(partitionStateSerializer, DEFAULT_PUBSUB_CONTEXT_FOR_UNIT_TESTING);
    offsetRecord.setOffsetLag(1000L);
    doReturn(offsetRecord).when(storageMetadataService).getLastOffset(any(), anyInt(), any());

    sslDir = Files.createTempDirectory("clientOriginSsl");
    TestSslMaterial sslMaterial = generateSslMaterial(sslDir);
    serverSslFactory = buildSslFactory(sslMaterial.serverKeyStorePath, sslMaterial.trustStorePath);
    clientSslFactory = buildSslFactory(sslMaterial.clientKeyStorePath, sslMaterial.trustStorePath);

    prepareSnapshotFile();
  }

  @AfterMethod
  public void tearDown() throws Exception {
    if (manager != null) {
      manager.close();
    }
    deleteRecursively(snapshotDir);
    deleteRecursively(partitionDir);
    deleteRecursively(sslDir);
  }

  @Test
  public void testClientOriginTransferSucceedsWhenAcceptedAndAclGranted() throws Exception {
    DynamicAccessController accessController = mockAccessController(true);
    manager = startManager(true, accessController);

    CompletionStage<InputStream> future =
        manager.get(STORE, VERSION, PARTITION, BlobTransferTableFormat.BLOCK_BASED_TABLE);
    future.toCompletableFuture().get(1, TimeUnit.MINUTES);

    Assert.assertTrue(Files.exists(destFile), "Client should have received the snapshot file from the server");
    Assert.assertEquals(Files.readAllBytes(destFile), Files.readAllBytes(srcFile));
    // The request was classified CLIENT-origin and went through the per-store read ACL.
    verify(accessController, atLeastOnce()).hasAccess(any(X509Certificate.class), anyString(), anyString());
  }

  @Test
  public void testClientOriginRejectedWhenAcceptFlagOff() throws Exception {
    DynamicAccessController accessController = mockAccessController(true);
    manager = startManager(false, accessController);

    assertTransferRejected();
  }

  @Test
  public void testClientOriginRejectedWhenAclDenied() throws Exception {
    DynamicAccessController accessController = mockAccessController(false);
    manager = startManager(true, accessController);

    assertTransferRejected();
    verify(accessController, atLeastOnce()).hasAccess(any(X509Certificate.class), anyString(), anyString());
  }

  private void assertTransferRejected() {
    try {
      manager.get(STORE, VERSION, PARTITION, BlobTransferTableFormat.BLOCK_BASED_TABLE)
          .toCompletableFuture()
          .get(10, TimeUnit.SECONDS);
      Assert.fail("Expected the client-origin request to be rejected by the server");
    } catch (ExecutionException expected) {
      // Server rejected the client-origin request with 403, so the transfer fails.
    } catch (InterruptedException | java.util.concurrent.TimeoutException e) {
      Assert.fail("Transfer did not fail promptly: " + e);
    }
    Assert.assertTrue(Files.notExists(destFile), "No file should be transferred when the request is rejected");
  }

  /**
   * Boot the real blob-transfer server (with the given accept flag and access controller) and a client that presents a
   * distinct certificate, wired through the real {@link NettyP2PBlobTransferManager} server-fallback path with no peer.
   */
  private NettyP2PBlobTransferManager startManager(
      boolean acceptClientRequest,
      DynamicAccessController accessController) throws Exception {
    int port = TestUtils.getFreePort();
    GlobalChannelTrafficShapingHandler trafficHandler = getGlobalChannelTrafficShapingHandlerInstance(2000000, 2000000);

    StorageEngineRepository storageEngineRepository = mock(StorageEngineRepository.class);
    StorageEngine storageEngine = mock(StorageEngine.class);
    doReturn(storageEngine).when(storageEngineRepository).getLocalStorageEngine(anyString());
    doReturn(true).when(storageEngine).containsPartition(anyInt());
    BlobSnapshotManager blobSnapshotManager =
        spy(new BlobSnapshotManager(storageEngineRepository, storageMetadataService));
    doNothing().when(blobSnapshotManager).createSnapshot(anyString(), anyInt());

    Optional<BlobTransferAclHandler> aclHandler =
        createAclHandler(aclConfigLoader(), Optional.of(accessController), acceptClientRequest);

    server = new P2PBlobTransferService(
        port,
        snapshotDir.toString(),
        MAX_TRANSFER_TIMEOUT_MIN,
        blobSnapshotManager,
        trafficHandler,
        blobTransferStats,
        Optional.of(serverSslFactory),
        aclHandler,
        20,
        25,
        acceptClientRequest);

    NettyFileTransferClient client = new NettyFileTransferClient(
        port,
        partitionDir.toString(),
        storageMetadataService,
        30,
        60,
        MAX_TRANSFER_TIMEOUT_MIN,
        Math.max(4, Runtime.getRuntime().availableProcessors() / 5),
        trafficHandler,
        blobTransferStats,
        Optional.of(clientSslFactory),
        () -> notifier,
        LogContext.forTests(VeniceComponent.DAVINCI_CLIENT.name()));

    BlobFinder peerFinder = mock(BlobFinder.class);
    BlobPeersDiscoveryResponse discoveryResponse = new BlobPeersDiscoveryResponse();
    discoveryResponse.setDiscoveryResult(Collections.singletonList("localhost"));
    doReturn(discoveryResponse).when(peerFinder).discoverBlobPeers(anyString(), anyInt(), anyInt());

    NettyP2PBlobTransferManager blobTransferManager = new NettyP2PBlobTransferManager(
        server,
        client,
        peerFinder,
        partitionDir.toString(),
        versionedBlobTransferStats,
        5,
        LogContext.forTests(VeniceComponent.DAVINCI_CLIENT.name()));
    blobTransferManager.start();
    return blobTransferManager;
  }

  private DynamicAccessController mockAccessController(boolean grant) throws Exception {
    DynamicAccessController accessController = mock(DynamicAccessController.class);
    when(accessController.hasAccess(any(X509Certificate.class), anyString(), anyString())).thenReturn(grant);
    return accessController;
  }

  private VeniceConfigLoader aclConfigLoader() {
    Properties properties = new Properties();
    properties.setProperty(ConfigKeys.BLOB_TRANSFER_SSL_ENABLED, "true");
    properties.setProperty(ConfigKeys.BLOB_TRANSFER_ACL_ENABLED, "true");
    properties.setProperty(ConfigKeys.IDENTITY_PARSER_CLASS, TestBlobTransferIdentityParser.class.getName());
    VeniceConfigLoader configLoader = mock(VeniceConfigLoader.class);
    when(configLoader.getCombinedProperties()).thenReturn(new VeniceProperties(properties));
    return configLoader;
  }

  private static SSLFactory buildSslFactory(Path keyStorePath, Path trustStorePath) throws Exception {
    Properties properties = new Properties();
    properties.setProperty(SSL_ENABLED, "true");
    properties.setProperty(SSL_KEYSTORE_TYPE, "JKS");
    properties.setProperty(SSL_KEYSTORE_LOCATION, keyStorePath.toString());
    properties.setProperty(SSL_KEYSTORE_PASSWORD, KEYSTORE_PASSWORD);
    properties.setProperty(SSL_TRUSTSTORE_TYPE, "JKS");
    properties.setProperty(SSL_TRUSTSTORE_LOCATION, trustStorePath.toString());
    properties.setProperty(SSL_TRUSTSTORE_PASSWORD, KEYSTORE_PASSWORD);
    properties.setProperty(SSL_KEY_PASSWORD, KEYSTORE_PASSWORD);
    properties.setProperty(SSL_KEYMANAGER_ALGORITHM, "SunX509");
    properties.setProperty(SSL_TRUSTMANAGER_ALGORITHM, "SunX509");
    properties.setProperty(SSL_SECURE_RANDOM_IMPLEMENTATION, "SHA1PRNG");
    return new DefaultSSLFactory(properties);
  }

  private static TestSslMaterial generateSslMaterial(Path outputDir) throws Exception {
    char[] password = KEYSTORE_PASSWORD.toCharArray();
    KeyPair caKeyPair = generateRsaKeyPair();
    X500Name caSubject = new X500Name("CN=venice-blob-test-ca, O=linkedin");
    X509Certificate caCertificate = generateCertificate(caSubject, caKeyPair.getPrivate(), caSubject, caKeyPair, true);

    KeyPair serverKeyPair = generateRsaKeyPair();
    X509Certificate serverCertificate = generateCertificate(
        caSubject,
        caKeyPair.getPrivate(),
        new X500Name("CN=venice-server, O=linkedin"),
        serverKeyPair,
        false);

    KeyPair clientKeyPair = generateRsaKeyPair();
    X509Certificate clientCertificate = generateCertificate(
        caSubject,
        caKeyPair.getPrivate(),
        new X500Name("CN=venice-client, O=linkedin"),
        clientKeyPair,
        false);

    Path serverKeyStorePath = outputDir.resolve("blob-transfer-server.jks");
    Path clientKeyStorePath = outputDir.resolve("blob-transfer-client.jks");
    Path trustStorePath = outputDir.resolve("blob-transfer-truststore.jks");
    writeKeyStore(serverKeyStorePath, "server", serverKeyPair.getPrivate(), serverCertificate, caCertificate, password);
    writeKeyStore(clientKeyStorePath, "client", clientKeyPair.getPrivate(), clientCertificate, caCertificate, password);
    writeTrustStore(trustStorePath, caCertificate, password);
    return new TestSslMaterial(serverKeyStorePath, clientKeyStorePath, trustStorePath);
  }

  private static KeyPair generateRsaKeyPair() throws Exception {
    KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
    keyPairGenerator.initialize(2048);
    return keyPairGenerator.generateKeyPair();
  }

  private static X509Certificate generateCertificate(
      X500Name issuer,
      PrivateKey issuerPrivateKey,
      X500Name subject,
      KeyPair subjectKeyPair,
      boolean isCa) throws Exception {
    Date notBefore = new Date(System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(1));
    Date notAfter = new Date(System.currentTimeMillis() + TimeUnit.DAYS.toMillis(1));
    JcaX509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(
        issuer,
        new BigInteger(160, SECURE_RANDOM),
        notBefore,
        notAfter,
        subject,
        subjectKeyPair.getPublic());
    builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(isCa));
    ContentSigner signer = new JcaContentSignerBuilder("SHA256withRSA").build(issuerPrivateKey);
    return new JcaX509CertificateConverter().getCertificate(builder.build(signer));
  }

  private static void writeKeyStore(
      Path keyStorePath,
      String alias,
      PrivateKey privateKey,
      X509Certificate certificate,
      X509Certificate caCertificate,
      char[] password) throws Exception {
    KeyStore keyStore = KeyStore.getInstance("JKS");
    keyStore.load(null, password);
    keyStore.setKeyEntry(alias, privateKey, password, new Certificate[] { certificate, caCertificate });
    try (OutputStream outputStream = Files.newOutputStream(keyStorePath)) {
      keyStore.store(outputStream, password);
    }
  }

  private static void writeTrustStore(Path trustStorePath, X509Certificate caCertificate, char[] password)
      throws Exception {
    KeyStore trustStore = KeyStore.getInstance("JKS");
    trustStore.load(null, password);
    trustStore.setCertificateEntry("venice-blob-test-ca", caCertificate);
    try (OutputStream outputStream = Files.newOutputStream(trustStorePath)) {
      trustStore.store(outputStream, password);
    }
  }

  private static final class TestSslMaterial {
    private final Path serverKeyStorePath;
    private final Path clientKeyStorePath;
    private final Path trustStorePath;

    private TestSslMaterial(Path serverKeyStorePath, Path clientKeyStorePath, Path trustStorePath) {
      this.serverKeyStorePath = serverKeyStorePath;
      this.clientKeyStorePath = clientKeyStorePath;
      this.trustStorePath = trustStorePath;
    }
  }

  public static class TestBlobTransferIdentityParser implements IdentityParser {
    @Override
    public String parseIdentityFromCert(X509Certificate certificate) {
      String subjectName = certificate.getSubjectX500Principal().getName();
      if (subjectName.contains("CN=venice-client")) {
        return "venice-client";
      }
      if (subjectName.contains("CN=venice-server")) {
        return "venice-server";
      }
      return subjectName;
    }
  }

  private void prepareSnapshotFile() throws Exception {
    Path versionSnapshotDir =
        Paths.get(RocksDBUtils.composeSnapshotDir(snapshotDir.toString(), STORE + "_v" + VERSION, PARTITION));
    Path versionPartitionDir =
        Paths.get(RocksDBUtils.composePartitionDbDir(partitionDir.toString(), STORE + "_v" + VERSION, PARTITION));
    Files.createDirectories(versionSnapshotDir);
    srcFile = versionSnapshotDir.resolve("snapshot.sst");
    destFile = versionPartitionDir.resolve("snapshot.sst");
    Files.write(srcFile, "client-origin-blob-payload".getBytes(StandardCharsets.UTF_8));
    Assert.assertTrue(Files.notExists(destFile));
  }

  private static void deleteRecursively(Path dir) throws Exception {
    if (Files.exists(dir)) {
      Files.walk(dir).sorted(Comparator.reverseOrder()).forEach(path -> {
        try {
          Files.delete(path);
        } catch (Exception e) {
          // best effort cleanup
        }
      });
    }
  }
}
