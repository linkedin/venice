package com.linkedin.venice.fastclient.meta;

import static com.linkedin.venice.ConfigKeys.SERVER_HTTP2_INBOUND_ENABLED;
import static org.testng.Assert.*;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compression.VeniceCompressor;
import com.linkedin.venice.fastclient.ClientConfig;
import com.linkedin.venice.fastclient.stats.ClusterStats;
import com.linkedin.venice.fastclient.utils.ClientTestUtils;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.meta.OnlineInstanceFinder;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class RequestBasedMetadataIntegrationTest {
  private static final int KEY_COUNT = 100;
  private static final long TIME_OUT = 60 * Time.MS_PER_SECOND;

  private final VenicePartitioner defaultPartitioner = new DefaultVenicePartitioner();
  private VeniceClusterWrapper veniceCluster;
  private String storeName;
  private RequestBasedMetadata requestBasedMetadata;
  private RecordSerializer<Object> keySerializer;
  private Client r2Client;
  private D2Client d2Client;
  private ClientConfig clientConfig;

  @BeforeClass
  public void setUp() throws Exception {
    Utils.thisIsLocalhost();
    Properties props = new Properties();
    props.put(SERVER_HTTP2_INBOUND_ENABLED, "true");
    veniceCluster = ServiceFactory.getVeniceCluster(1, 2, 1, 2, 100, true, false, props);
    r2Client = ClientTestUtils.getR2Client();
    d2Client = D2TestUtils.getAndStartHttpsD2Client(veniceCluster.getZk().getAddress());
    storeName = veniceCluster.createStore(KEY_COUNT);

    keySerializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(Schema.parse(VeniceClusterWrapper.DEFAULT_KEY_SCHEMA));

    // Populate required ClientConfig fields for initializing RequestBasedMetadata
    ClientConfig.ClientConfigBuilder clientConfigBuilder = new ClientConfig.ClientConfigBuilder();
    clientConfigBuilder.setStoreName(storeName);
    clientConfigBuilder.setR2Client(r2Client);
    clientConfigBuilder.setMetricsRepository(new MetricsRepository());
    clientConfigBuilder.setSpeculativeQueryEnabled(true);
    clientConfigBuilder.setMetadataRefreshIntervalInSeconds(1);
    clientConfig = clientConfigBuilder.build();

    requestBasedMetadata = new RequestBasedMetadata(
        clientConfig,
        new D2TransportClient(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME, d2Client));
    requestBasedMetadata.start();
  }

  @Test(timeOut = TIME_OUT)
  public void testMetadata() {
    VeniceRouterWrapper routerWrapper = veniceCluster.getRandomVeniceRouter();
    ReadOnlyStoreRepository storeRepository = routerWrapper.getMetaDataRepository();
    OnlineInstanceFinder onlineInstanceFinder = routerWrapper.getRoutingDataRepository();
    assertEquals(
        requestBasedMetadata.getCurrentStoreVersion(),
        storeRepository.getStore(storeName).getCurrentVersion());
    List<Version> versions = storeRepository.getStore(storeName).getVersions();
    assertFalse(versions.isEmpty(), "Version list cannot be empty.");
    byte[] keyBytes = keySerializer.serialize(1);
    for (Version version: versions) {
      verifyMetadata(onlineInstanceFinder, version.getNumber(), version.getPartitionCount(), keyBytes);
    }
    // Make two new versions before checking the metadata again
    veniceCluster.createVersion(storeName, KEY_COUNT);
    veniceCluster.createVersion(storeName, KEY_COUNT);

    TestUtils.waitForNonDeterministicAssertion(
        30,
        TimeUnit.SECONDS,
        () -> assertEquals(
            requestBasedMetadata.getCurrentStoreVersion(),
            storeRepository.getStore(storeName).getCurrentVersion()));
    versions = storeRepository.getStore(storeName).getVersions();
    assertFalse(versions.isEmpty(), "Version list cannot be empty.");
    for (Version version: versions) {
      verifyMetadata(onlineInstanceFinder, version.getNumber(), version.getPartitionCount(), keyBytes);
    }
  }

  @Test(timeOut = TIME_OUT)
  public void testMetadataSchemaRetriever() {
    ReadOnlySchemaRepository schemaRepository = veniceCluster.getRandomVeniceRouter().getSchemaRepository();
    assertEquals(requestBasedMetadata.getKeySchema(), schemaRepository.getKeySchema(storeName).getSchema());
    SchemaEntry latestValueSchema = schemaRepository.getSupersetOrLatestValueSchema(storeName);
    assertEquals(requestBasedMetadata.getLatestValueSchemaId().intValue(), latestValueSchema.getId());
    assertEquals(requestBasedMetadata.getLatestValueSchema(), latestValueSchema.getSchema());
    assertEquals(requestBasedMetadata.getValueSchema(latestValueSchema.getId()), latestValueSchema.getSchema());
    assertEquals(requestBasedMetadata.getValueSchemaId(latestValueSchema.getSchema()), latestValueSchema.getId());
  }

  @Test(timeOut = TIME_OUT)
  public void testMetadataZstdDictionaryFetch() {
    String zstdStoreName = veniceCluster.createStoreWithZstdDictionary(KEY_COUNT);

    ClientConfig.ClientConfigBuilder clientConfigBuilder = new ClientConfig.ClientConfigBuilder();
    clientConfigBuilder.setStoreName(zstdStoreName);
    clientConfigBuilder.setR2Client(r2Client);
    clientConfigBuilder.setMetricsRepository(new MetricsRepository());
    clientConfigBuilder.setSpeculativeQueryEnabled(true);
    clientConfigBuilder.setMetadataRefreshIntervalInSeconds(1);
    ClientConfig zstdClientConfig = clientConfigBuilder.build();

    RequestBasedMetadata zstdRequestBasedMetadata = new RequestBasedMetadata(
        zstdClientConfig,
        new D2TransportClient(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME, d2Client));
    zstdRequestBasedMetadata.start();

    VeniceRouterWrapper routerWrapper = veniceCluster.getRandomVeniceRouter();
    ReadOnlyStoreRepository storeRepository = routerWrapper.getMetaDataRepository();
    TestUtils.waitForNonDeterministicAssertion(
        30,
        TimeUnit.SECONDS,
        () -> assertEquals(
            zstdRequestBasedMetadata.getCurrentStoreVersion(),
            storeRepository.getStore(zstdStoreName).getCurrentVersion()));
    VeniceCompressor compressor = zstdRequestBasedMetadata
        .getCompressor(CompressionStrategy.ZSTD_WITH_DICT, storeRepository.getStore(zstdStoreName).getCurrentVersion());
    assertNotNull(compressor);
    ClusterStats clusterStats = zstdClientConfig.getClusterStats();
    List<Double> version_update_failure = clusterStats.getMetricValues("version_update_failure", "OccurrenceRate");
    List<Double> current_version = clusterStats.getMetricValues("current_version", "Gauge");
    assertEquals(version_update_failure.size(), 1, "Unexpected statistic size");
    assertEquals(version_update_failure.get(0), 0.0, "Unexpected version update failure");
    assertEquals(current_version.size(), 1, "Unexpected statistic size");
    assertEquals(current_version.get(0), 1.0, "Unexpected version number");
  }

  private void verifyMetadata(
      OnlineInstanceFinder onlineInstanceFinder,
      int versionNumber,
      int partitionCount,
      byte[] key) {
    final String resourceName = Version.composeKafkaTopic(storeName, versionNumber);
    final int partitionId = ThreadLocalRandom.current().nextInt(0, partitionCount);
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      assertEquals(
          defaultPartitioner.getPartitionId(key, partitionCount),
          requestBasedMetadata.getPartitionId(versionNumber, key));
      Set<String> routerReadyToServeView = onlineInstanceFinder.getReadyToServeInstances(resourceName, partitionId)
          .stream()
          .map(instance -> instance.getUrl(true))
          .collect(Collectors.toSet());
      Set<String> metadataView = new HashSet<>(requestBasedMetadata.getReplicas(versionNumber, partitionId));
      assertEquals(
          metadataView.size(),
          routerReadyToServeView.size(),
          "Different number of ready to serve instances between router and StoreMetadata.");
      for (String instance: routerReadyToServeView) {
        assertTrue(metadataView.contains(instance), "Instance: " + instance + " is missing from StoreMetadata.");
      }
    });
  }

  @AfterClass
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(requestBasedMetadata);
    if (d2Client != null) {
      D2ClientUtils.shutdownClient(d2Client);
    }
    if (r2Client != null) {
      r2Client.shutdown(null);
    }
    Utils.closeQuietlyWithErrorLogged(veniceCluster);
  }
}
