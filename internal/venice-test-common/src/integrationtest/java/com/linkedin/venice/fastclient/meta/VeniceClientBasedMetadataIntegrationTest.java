package com.linkedin.venice.fastclient.meta;

import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_STORE_NAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.fastclient.ClientConfig;
import com.linkedin.venice.fastclient.utils.ClientTestUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
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
import com.linkedin.venice.system.store.MetaStoreDataType;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.SslUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class VeniceClientBasedMetadataIntegrationTest {
  protected static final int KEY_COUNT = 100;
  protected static final long TIME_OUT = 60 * Time.MS_PER_SECOND;

  private final VenicePartitioner defaultPartitioner = new DefaultVenicePartitioner();

  protected ClientConfig clientConfig;
  protected String storeName;
  protected VeniceClusterWrapper veniceCluster;
  protected VeniceClientBasedMetadata veniceClientBasedMetadata;

  private RecordSerializer<Object> keySerializer;
  private Client r2Client;
  private AvroSpecificStoreClient<StoreMetaKey, StoreMetaValue> thinClientForMetaStore = null;

  @BeforeClass
  public void setUp() throws Exception {
    Utils.thisIsLocalhost();
    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .numberOfServers(2)
        .numberOfRouters(1)
        .replicationFactor(2)
        .partitionSize(100)
        .minActiveReplica(1)
        .sslToStorageNodes(true)
        .sslToKafka(false)
        .build();
    veniceCluster = ServiceFactory.getVeniceCluster(options);
    r2Client = ClientTestUtils.getR2Client();
    createStore();
    String metaSystemStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName);
    veniceCluster.useControllerClient(controllerClient -> {
      VersionCreationResponse metaSystemStoreVersionCreationResponse =
          controllerClient.emptyPush(metaSystemStoreName, "test_bootstrap_meta_system_store", 10000);
      assertFalse(
          metaSystemStoreVersionCreationResponse.isError(),
          "New version creation for meta system store failed with error: "
              + metaSystemStoreVersionCreationResponse.getError());
      TestUtils.waitForNonDeterministicPushCompletion(
          metaSystemStoreVersionCreationResponse.getKafkaTopic(),
          controllerClient,
          30,
          TimeUnit.SECONDS);
    });
    thinClientForMetaStore = com.linkedin.venice.client.store.ClientFactory.getAndStartSpecificAvroClient(
        com.linkedin.venice.client.store.ClientConfig
            .defaultSpecificClientConfig(metaSystemStoreName, StoreMetaValue.class)
            .setVeniceURL(veniceCluster.getRandomRouterURL())
            .setSslFactory(SslUtils.getVeniceLocalSslFactory()));
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      Assert.assertNotNull(
          thinClientForMetaStore
              .get(
                  MetaStoreDataType.STORE_CLUSTER_CONFIG
                      .getStoreMetaKey(Collections.singletonMap(KEY_STRING_STORE_NAME, storeName)))
              .get());
    });
    keySerializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(Schema.parse(VeniceClusterWrapper.DEFAULT_KEY_SCHEMA));

    // Populate required ClientConfig fields for initializing DaVinciClientBasedMetadata
    ClientConfig.ClientConfigBuilder clientConfigBuilder = new ClientConfig.ClientConfigBuilder<>();
    clientConfigBuilder.setStoreName(storeName);
    clientConfigBuilder.setR2Client(r2Client);
    clientConfigBuilder.setMetricsRepository(new MetricsRepository());
    clientConfigBuilder.setSpeculativeQueryEnabled(true);
    clientConfigBuilder.setMetadataRefreshIntervalInSeconds(1); // Faster refreshes for faster tests
    clientConfig = clientConfigBuilder.build();
    veniceClientBasedMetadata = new ThinClientBasedMetadata(clientConfig, thinClientForMetaStore);
    veniceClientBasedMetadata.start();
  }

  protected void createStore() throws Exception {
    storeName = veniceCluster.createStore(KEY_COUNT);
  }

  @Test(timeOut = TIME_OUT)
  public void testMetadata() {
    VeniceRouterWrapper routerWrapper = veniceCluster.getRandomVeniceRouter();
    ReadOnlyStoreRepository storeRepository = routerWrapper.getMetaDataRepository();
    OnlineInstanceFinder onlineInstanceFinder = routerWrapper.getRoutingDataRepository();
    assertEquals(
        veniceClientBasedMetadata.getCurrentStoreVersion(),
        storeRepository.getStore(storeName).getCurrentVersion());
    List<Version> versions = storeRepository.getStore(storeName).getVersions();
    assertFalse(versions.isEmpty(), "Version list cannot be empty.");
    byte[] keyBytes = keySerializer.serialize(1);
    for (Version version: versions) {
      verifyMetadata(
          veniceClientBasedMetadata,
          onlineInstanceFinder,
          version.getNumber(),
          version.getPartitionCount(),
          keyBytes);
    }
    // Make two new versions before checking the metadata again
    veniceCluster.createVersion(storeName, KEY_COUNT);
    veniceCluster.createVersion(storeName, KEY_COUNT);

    TestUtils.waitForNonDeterministicAssertion(
        30,
        TimeUnit.SECONDS,
        () -> assertEquals(
            veniceClientBasedMetadata.getCurrentStoreVersion(),
            storeRepository.getStore(storeName).getCurrentVersion()));
    versions = storeRepository.getStore(storeName).getVersions();
    assertFalse(versions.isEmpty(), "Version list cannot be empty.");
    for (Version version: versions) {
      verifyMetadata(
          veniceClientBasedMetadata,
          onlineInstanceFinder,
          version.getNumber(),
          version.getPartitionCount(),
          keyBytes);
    }
  }

  @Test(timeOut = TIME_OUT)
  public void testMetadataSchemaRetriever() {
    ReadOnlySchemaRepository schemaRepository = veniceCluster.getRandomVeniceRouter().getSchemaRepository();
    assertEquals(veniceClientBasedMetadata.getKeySchema(), schemaRepository.getKeySchema(storeName).getSchema());
    SchemaEntry latestValueSchema = schemaRepository.getSupersetOrLatestValueSchema(storeName);
    assertEquals(veniceClientBasedMetadata.getLatestValueSchemaId().intValue(), latestValueSchema.getId());
    assertEquals(veniceClientBasedMetadata.getLatestValueSchema(), latestValueSchema.getSchema());
    assertEquals(veniceClientBasedMetadata.getValueSchema(latestValueSchema.getId()), latestValueSchema.getSchema());
    assertEquals(veniceClientBasedMetadata.getValueSchemaId(latestValueSchema.getSchema()), latestValueSchema.getId());
  }

  @Test(timeOut = TIME_OUT, dataProvider = "True-and-False", dataProviderClass = DataProviderUtils.class)
  public void testMetaSystemStoreVersionBump(boolean useThinClientBasedMetadata) {
    // Bump the underlying system store version twice and make sure DaVinciClientBasedMetadata is still subscribed to
    // the correct meta system store version.
    String metaSystemStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName);
    veniceCluster.useControllerClient(controllerClient -> {
      for (int i = 0; i < 2; i++) {
        VersionCreationResponse metaSystemStoreVersionCreationResponse =
            controllerClient.emptyPush(metaSystemStoreName, "test_meta_system_store_bump_" + i, 10000);
        assertFalse(
            metaSystemStoreVersionCreationResponse.isError(),
            "New version push for meta system store failed with error: "
                + metaSystemStoreVersionCreationResponse.getError());
        TestUtils.waitForNonDeterministicPushCompletion(
            metaSystemStoreVersionCreationResponse.getKafkaTopic(),
            controllerClient,
            30,
            TimeUnit.SECONDS);
      }
    });
    // Make a new version and check the metadata
    veniceCluster.useControllerClient(controllerClient -> {
      controllerClient.emptyPush(storeName, "test_meta_system_store_bump_user_push", 10000);
    });
    ReadOnlyStoreRepository storeRepository = veniceCluster.getRandomVeniceRouter().getMetaDataRepository();
    TestUtils.waitForNonDeterministicAssertion(
        30,
        TimeUnit.SECONDS,
        () -> assertEquals(
            veniceClientBasedMetadata.getCurrentStoreVersion(),
            storeRepository.getStore(storeName).getCurrentVersion()));
  }

  private void verifyMetadata(
      VeniceClientBasedMetadata veniceClientBasedMetadata,
      OnlineInstanceFinder onlineInstanceFinder,
      int versionNumber,
      int partitionCount,
      byte[] key) {
    final String resourceName = Version.composeKafkaTopic(storeName, versionNumber);
    final int partitionId = ThreadLocalRandom.current().nextInt(0, partitionCount);
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      assertEquals(
          defaultPartitioner.getPartitionId(key, partitionCount),
          veniceClientBasedMetadata.getPartitionId(versionNumber, key));
      Set<String> routerReadyToServeView = onlineInstanceFinder.getReadyToServeInstances(resourceName, partitionId)
          .stream()
          .map(instance -> instance.getUrl(true))
          .collect(Collectors.toSet());
      Set<String> metadataView = new HashSet<>(veniceClientBasedMetadata.getReplicas(versionNumber, partitionId));
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
    Utils.closeQuietlyWithErrorLogged(veniceClientBasedMetadata);
    if (r2Client != null) {
      r2Client.shutdown(null);
    }
    Utils.closeQuietlyWithErrorLogged(veniceCluster);
  }
}
