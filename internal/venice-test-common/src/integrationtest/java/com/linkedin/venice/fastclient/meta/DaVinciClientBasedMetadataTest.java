package com.linkedin.venice.fastclient.meta;

import static com.linkedin.venice.ConfigKeys.CLIENT_USE_DA_VINCI_BASED_SYSTEM_STORE_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.CLIENT_USE_SYSTEM_STORE_REPOSITORY;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.meta.PersistenceType.ROCKS_DB;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.D2.D2ClientUtils;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.fastclient.ClientConfig;
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
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class DaVinciClientBasedMetadataTest {
  protected static final int KEY_COUNT = 100;
  protected static final long TIME_OUT = 60 * Time.MS_PER_SECOND;

  private final VenicePartitioner defaultPartitioner = new DefaultVenicePartitioner();

  protected VeniceClusterWrapper veniceCluster;
  protected String storeName;
  private VeniceProperties daVinciBackendConfig;
  protected DaVinciClientBasedMetadata daVinciClientBasedMetadata;
  private RecordSerializer<Object> keySerializer;
  private Client r2Client;
  private D2Client d2Client;
  private CachingDaVinciClientFactory daVinciClientFactory;
  private DaVinciClient<StoreMetaKey, StoreMetaValue> daVinciClientForMetaStore;
  protected ClientConfig clientConfig;

  @BeforeClass
  public void setUp() throws Exception {
    veniceCluster = ServiceFactory.getVeniceCluster(1, 2, 1, 2, 100, true, false);
    r2Client = ClientTestUtils.getR2Client();
    d2Client = D2TestUtils.getAndStartD2Client(veniceCluster.getZk().getAddress());
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
      daVinciBackendConfig = new PropertyBuilder().put(DATA_BASE_PATH, Utils.getTempDataDirectory().getAbsolutePath())
          .put(PERSISTENCE_TYPE, ROCKS_DB)
          .put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
          .put(CLIENT_USE_DA_VINCI_BASED_SYSTEM_STORE_REPOSITORY, true)
          .build();
    });

    daVinciClientFactory = new CachingDaVinciClientFactory(
        d2Client,
        VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME,
        new MetricsRepository(),
        daVinciBackendConfig);
    daVinciClientForMetaStore = daVinciClientFactory.getAndStartSpecificAvroClient(
        VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName),
        new DaVinciConfig(),
        StoreMetaValue.class);
    keySerializer =
        SerializerDeserializerFactory.getAvroGenericSerializer(Schema.parse(VeniceClusterWrapper.DEFAULT_KEY_SCHEMA));

    // Populate required ClientConfig fields for initializing DaVinciClientBasedMetadata
    ClientConfig.ClientConfigBuilder clientConfigBuilder = new ClientConfig.ClientConfigBuilder<>();
    clientConfigBuilder.setStoreName(storeName);
    clientConfigBuilder.setR2Client(r2Client);
    clientConfigBuilder.setMetricsRepository(new MetricsRepository());
    clientConfigBuilder.setSpeculativeQueryEnabled(true);
    clientConfigBuilder.setDaVinciClientForMetaStore(daVinciClientForMetaStore);
    clientConfigBuilder.setMetadataRefreshIntervalInSeconds(1); // Faster refreshes for faster tests
    clientConfig = clientConfigBuilder.build();
    daVinciClientBasedMetadata = new DaVinciClientBasedMetadata(clientConfig);
    daVinciClientBasedMetadata.start();
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
        daVinciClientBasedMetadata.getCurrentStoreVersion(),
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
            daVinciClientBasedMetadata.getCurrentStoreVersion(),
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
    assertEquals(daVinciClientBasedMetadata.getKeySchema(), schemaRepository.getKeySchema(storeName).getSchema());
    SchemaEntry latestValueSchema = schemaRepository.getSupersetOrLatestValueSchema(storeName);
    assertEquals(daVinciClientBasedMetadata.getLatestValueSchemaId().intValue(), latestValueSchema.getId());
    assertEquals(daVinciClientBasedMetadata.getLatestValueSchema(), latestValueSchema.getSchema());
    assertEquals(daVinciClientBasedMetadata.getValueSchema(latestValueSchema.getId()), latestValueSchema.getSchema());
    assertEquals(daVinciClientBasedMetadata.getValueSchemaId(latestValueSchema.getSchema()), latestValueSchema.getId());
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
          daVinciClientBasedMetadata.getPartitionId(versionNumber, key));
      Set<String> routerReadyToServeView = onlineInstanceFinder.getReadyToServeInstances(resourceName, partitionId)
          .stream()
          .map(instance -> instance.getUrl(true))
          .collect(Collectors.toSet());
      Set<String> metadataView = new HashSet<>(daVinciClientBasedMetadata.getReplicas(versionNumber, partitionId));
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
    Utils.closeQuietlyWithErrorLogged(daVinciClientBasedMetadata);
    Utils.closeQuietlyWithErrorLogged(daVinciClientForMetaStore);
    Utils.closeQuietlyWithErrorLogged(daVinciClientFactory);
    if (d2Client != null) {
      D2ClientUtils.shutdownClient(d2Client);
    }
    if (r2Client != null) {
      r2Client.shutdown(null);
    }
    Utils.closeQuietlyWithErrorLogged(veniceCluster);
  }

}
