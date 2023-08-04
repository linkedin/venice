package com.linkedin.venice.fastclient.grpc;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE;
import static com.linkedin.venice.ConfigKeys.PARTICIPANT_MESSAGE_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_HTTP2_INBOUND_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_QUOTA_ENFORCEMENT_ENABLED;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.fastclient.ClientConfig.ClientConfigBuilder;
import com.linkedin.venice.fastclient.meta.StoreMetadataFetchMode;
import com.linkedin.venice.fastclient.utils.ClientTestUtils;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.util.Utf8;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class VeniceGrpcEndToEndTest {
  public static final int maxAllowedKeys = 150;
  private static final Logger LOGGER = LogManager.getLogger(VeniceGrpcEndToEndTest.class);
  private static final int recordCnt = 3000;
  private VeniceClusterWrapper cluster;
  private Map<String, String> nettyToGrpcPortMap;
  private String storeName;

  public VeniceClusterWrapper getCluster() {
    return cluster;
  }

  @BeforeClass
  public void setUp() throws Exception {
    Utils.thisIsLocalhost();

    Properties props = new Properties();
    props.setProperty(PARTICIPANT_MESSAGE_STORE_ENABLED, "false");
    props.setProperty(CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE, "true");
    props.put(SERVER_HTTP2_INBOUND_ENABLED, "true");
    props.put(SERVER_QUOTA_ENFORCEMENT_ENABLED, "true");

    cluster = ServiceFactory.getVeniceCluster(
        new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
            .partitionSize(1000)
            .numberOfPartitions(2)
            .maxNumberOfPartitions(5)
            .minActiveReplica(1)
            .numberOfRouters(1)
            .numberOfServers(2)
            .sslToStorageNodes(true)
            .enableGrpc(true)
            .extraProperties(props)
            .build());

    nettyToGrpcPortMap = cluster.getNettyToGrpcServerMap();
    storeName = writeData("testStore");
  }

  @AfterClass
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(cluster);
  }

  public String writeData(String storeName) throws IOException {
    // 1. Create a new store in Venice
    cluster.getNewStore(storeName);
    UpdateStoreQueryParams params = new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA);

    ControllerResponse updateStoreResponse = cluster.updateStore(storeName, params);
    Assert.assertNull(updateStoreResponse.getError());

    // 2. Write data to the store w/ writeSimpleAvroFileWithUserSchema
    File inputDir = TestWriteUtils.getTempDataDirectory();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    TestWriteUtils.writeSimpleAvroFileWithUserSchema(inputDir, false, recordCnt);

    // 3. Run a push job to push the data to Venice (VPJ)
    Properties vpjProps = TestWriteUtils.defaultVPJProps(cluster.getRandomRouterURL(), inputDirPath, storeName);
    TestWriteUtils.runPushJob("test push job", vpjProps);

    cluster.useControllerClient(
        controllerClient -> TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
          StoreResponse storeResponse = controllerClient.getStore(storeName);
          Assert.assertEquals(storeResponse.getStore().getCurrentVersion(), 1);
        }));

    return storeName;
  }

  private AvroGenericStoreClient<String, GenericRecord> getGenericFastClient(
      ClientConfigBuilder<Object, Object, SpecificRecord> clientConfigBuilder,
      MetricsRepository metricsRepository,
      D2Client d2Client) {
    clientConfigBuilder.setStoreMetadataFetchMode(StoreMetadataFetchMode.SERVER_BASED_METADATA);
    clientConfigBuilder.setClusterDiscoveryD2Service(VeniceRouterWrapper.CLUSTER_DISCOVERY_D2_SERVICE_NAME);
    clientConfigBuilder.setMetadataRefreshIntervalInSeconds(1);
    clientConfigBuilder.setD2Client(d2Client);
    clientConfigBuilder.setMetricsRepository(metricsRepository);

    return com.linkedin.venice.fastclient.factory.ClientFactory
        .getAndStartGenericStoreClient(clientConfigBuilder.build());
  }

  @Test
  public void testReadData() throws Exception {
    // 4. Create thin client
    AvroGenericStoreClient<Object, Object> avroClient = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(cluster.getRandomRouterURL()));

    // 4. Create fastClient
    Client r2Client = ClientTestUtils.getR2Client(ClientTestUtils.FastClientHTTPVariant.HTTP_2_BASED_R2_CLIENT);
    D2Client d2Client = D2TestUtils.getAndStartHttpsD2Client(cluster.getZk().getAddress());

    ClientConfigBuilder<Object, Object, SpecificRecord> clientConfigBuilder =
        new com.linkedin.venice.fastclient.ClientConfig.ClientConfigBuilder<>().setStoreName(storeName)
            .setR2Client(r2Client)
            .setMaxAllowedKeyCntInBatchGetReq(maxAllowedKeys + 1)
            .setRoutingPendingRequestCounterInstanceBlockThreshold(maxAllowedKeys + 1)
            .setSpeculativeQueryEnabled(false)
            .setUseStreamingBatchGetAsDefault(false);

    AvroGenericStoreClient<String, GenericRecord> genericFastClient =
        getGenericFastClient(clientConfigBuilder, new MetricsRepository(), d2Client);

    Set<Set<String>> keySets = getKeySets();

    for (Set<String> keys: keySets) {
      Map<String, GenericRecord> fastClientRet = genericFastClient.batchGet(keys).get();

      for (String k: keys) {
        String thinClientRecord = avroClient.get(k).get().toString();
        String fastClientRecord = ((Utf8) fastClientRet.get(k)).toString();
        String fastClientSingleGet = ((Utf8) genericFastClient.get(k).get()).toString();

        LOGGER.info("thinClientRecord: " + thinClientRecord + " for key: " + k);
        LOGGER.info("fastClientRecord: " + fastClientRecord + " for key: " + k);
        LOGGER.info("fastClientSingleGet: " + fastClientSingleGet + " for key: " + k);

        Assert.assertEquals(thinClientRecord, fastClientRecord);
        Assert.assertEquals(thinClientRecord, fastClientSingleGet);
      }
    }

    avroClient.close();
    genericFastClient.close();
  }

  @Test
  public void testGrpcFastClient() throws Exception {
    // 4. Create thin client
    AvroGenericStoreClient<Object, Object> avroClient = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(cluster.getRandomRouterURL()));

    // 4. Create fastClient
    Client r2Client = ClientTestUtils.getR2Client(ClientTestUtils.FastClientHTTPVariant.HTTP_2_BASED_R2_CLIENT);
    Client grpcR2ClientPassthrough =
        ClientTestUtils.getR2Client(ClientTestUtils.FastClientHTTPVariant.HTTP_2_BASED_R2_CLIENT);

    D2Client d2Client = D2TestUtils.getAndStartHttpsD2Client(cluster.getZk().getAddress());

    ClientConfigBuilder<Object, Object, SpecificRecord> clientConfigBuilder =
        new com.linkedin.venice.fastclient.ClientConfig.ClientConfigBuilder<>().setStoreName(storeName)
            .setR2Client(r2Client)
            .setMaxAllowedKeyCntInBatchGetReq(maxAllowedKeys + 1)
            .setRoutingPendingRequestCounterInstanceBlockThreshold(maxAllowedKeys + 1)
            .setSpeculativeQueryEnabled(false)
            .setUseStreamingBatchGetAsDefault(true);

    ClientConfigBuilder<Object, Object, SpecificRecord> grpcClientConfigBuilder =
        new com.linkedin.venice.fastclient.ClientConfig.ClientConfigBuilder<>().setStoreName(storeName)
            .setUseGrpc(true)
            .setNettyServerToGrpcAddressMap(nettyToGrpcPortMap)
            .setR2Client(grpcR2ClientPassthrough)
            .setMaxAllowedKeyCntInBatchGetReq(maxAllowedKeys)
            .setRoutingPendingRequestCounterInstanceBlockThreshold(maxAllowedKeys)
            .setSpeculativeQueryEnabled(false)
            .setUseStreamingBatchGetAsDefault(true);

    AvroGenericStoreClient<String, GenericRecord> genericFastClient =
        getGenericFastClient(clientConfigBuilder, new MetricsRepository(), d2Client);

    AvroGenericStoreClient<String, GenericRecord> grpcFastClient =
        getGenericFastClient(grpcClientConfigBuilder, new MetricsRepository(), d2Client);

    Set<Set<String>> keySets = getKeySets();

    for (Set<String> keys: keySets) {
      Map<String, GenericRecord> grpcClientRet = grpcFastClient.batchGet(keys).get();
      Map<String, GenericRecord> fastClientRet = genericFastClient.batchGet(keys).get();

      for (String k: keys) {
        String grpcBatchGetRecord = ((Utf8) grpcClientRet.get(k)).toString();
        String grpcClientRecord = ((Utf8) grpcFastClient.get(k).get()).toString();
        String fastClientBatchRecord = ((Utf8) fastClientRet.get(k)).toString();
        String avroClientRecord = avroClient.get(k).get().toString();

        LOGGER.info("key: {}, thinClientRecord: {}", k, avroClientRecord);
        LOGGER.info("key: {}, grpcClientRecord: {}", k, grpcClientRecord);
        LOGGER.info("key: {}, grpcBatchGetRecord: {}", k, grpcBatchGetRecord);
        LOGGER.info("key: {}, fastClientBatchGetRecord: {}", k, fastClientBatchRecord);

        Assert.assertEquals(grpcClientRecord, avroClientRecord);
        Assert.assertEquals(grpcBatchGetRecord, avroClientRecord);
        Assert.assertEquals(grpcBatchGetRecord, fastClientBatchRecord);
      }
    }

    grpcFastClient.close();
    avroClient.close();
    genericFastClient.close();
  }

  @Test
  public void testSingleGet() throws Exception {
    AvroGenericStoreClient<Object, Object> avroClient = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(cluster.getRandomRouterURL()));

    Client r2Client = ClientTestUtils.getR2Client(ClientTestUtils.FastClientHTTPVariant.HTTP_2_BASED_R2_CLIENT);

    D2Client d2Client = D2TestUtils.getAndStartHttpsD2Client(cluster.getZk().getAddress());

    ClientConfigBuilder<Object, Object, SpecificRecord> clientConfigBuilder =
        new com.linkedin.venice.fastclient.ClientConfig.ClientConfigBuilder<>().setStoreName(storeName)
            .setR2Client(r2Client)
            .setMaxAllowedKeyCntInBatchGetReq(maxAllowedKeys + 1)
            .setRoutingPendingRequestCounterInstanceBlockThreshold(maxAllowedKeys + 1)
            .setSpeculativeQueryEnabled(false)
            .setUseStreamingBatchGetAsDefault(true);

    ClientConfigBuilder<Object, Object, SpecificRecord> grpcClientConfigBuilder =
        new com.linkedin.venice.fastclient.ClientConfig.ClientConfigBuilder<>().setStoreName(storeName)
            .setUseGrpc(true)
            .setR2Client(r2Client)
            .setNettyServerToGrpcAddressMap(nettyToGrpcPortMap)
            .setMaxAllowedKeyCntInBatchGetReq(maxAllowedKeys)
            .setRoutingPendingRequestCounterInstanceBlockThreshold(maxAllowedKeys)
            .setSpeculativeQueryEnabled(false)
            .setUseStreamingBatchGetAsDefault(true);

    AvroGenericStoreClient<String, GenericRecord> genericFastClient =
        getGenericFastClient(clientConfigBuilder, new MetricsRepository(), d2Client);

    AvroGenericStoreClient<String, GenericRecord> grpcFastClient =
        getGenericFastClient(grpcClientConfigBuilder, new MetricsRepository(), d2Client);

    long grpcTime = 0;
    long fastClientTime = 0;

    for (int key = 1; key < recordCnt; key++) {
      long start = System.currentTimeMillis();
      String grpcClientRecord = ((Utf8) grpcFastClient.get(Integer.toString(key)).get()).toString();
      long end = System.currentTimeMillis();
      grpcTime += end - start;

      start = System.currentTimeMillis();
      String fastClientRecord = ((Utf8) genericFastClient.get(Integer.toString(key)).get()).toString();
      end = System.currentTimeMillis();
      fastClientTime += end - start;
      String avroClientRecord = avroClient.get(Integer.toString(key)).get().toString();

      LOGGER.info("key: {}, thinClientRecord: {}", key, avroClientRecord);
      LOGGER.info("key: {}, grpcClientRecord: {}", key, grpcClientRecord);
      LOGGER.info("key: {}, fastClientRecord: {}", key, fastClientRecord);

      Assert.assertEquals(grpcClientRecord, avroClientRecord);
      Assert.assertEquals(grpcClientRecord, fastClientRecord);
    }
    LOGGER.info("benchmark for {} records, ###", recordCnt);
    LOGGER.info("grpcTime: {}", grpcTime);
    LOGGER.info("fastClientTime: {}", fastClientTime);

    grpcFastClient.close();
    avroClient.close();
  }

  @Test
  public void fastClientWithBatchStreaming() throws Exception {
    Client grpcR2Client = ClientTestUtils.getR2Client(ClientTestUtils.FastClientHTTPVariant.HTTP_2_BASED_R2_CLIENT);
    Client fastR2Client = ClientTestUtils.getR2Client(ClientTestUtils.FastClientHTTPVariant.HTTP_2_BASED_R2_CLIENT);

    D2Client grpcD2Client = D2TestUtils.getAndStartHttpsD2Client(cluster.getZk().getAddress());
    D2Client fastD2Client = D2TestUtils.getAndStartHttpsD2Client(cluster.getZk().getAddress());

    ClientConfigBuilder<Object, Object, SpecificRecord> clientConfigBuilder =
        new com.linkedin.venice.fastclient.ClientConfig.ClientConfigBuilder<>().setStoreName(storeName)
            .setR2Client(fastR2Client)
            .setMaxAllowedKeyCntInBatchGetReq(maxAllowedKeys + 1)
            .setRoutingPendingRequestCounterInstanceBlockThreshold(maxAllowedKeys + 1)
            .setSpeculativeQueryEnabled(false)
            .setUseStreamingBatchGetAsDefault(true);

    ClientConfigBuilder<Object, Object, SpecificRecord> grpcClientConfigBuilder =
        new com.linkedin.venice.fastclient.ClientConfig.ClientConfigBuilder<>().setStoreName(storeName)
            .setUseGrpc(true)
            .setR2Client(grpcR2Client)
            .setNettyServerToGrpcAddressMap(nettyToGrpcPortMap)
            .setMaxAllowedKeyCntInBatchGetReq(maxAllowedKeys + 1)
            .setRoutingPendingRequestCounterInstanceBlockThreshold(maxAllowedKeys + 1)
            .setSpeculativeQueryEnabled(false)
            .setUseStreamingBatchGetAsDefault(true);

    AvroGenericStoreClient<String, GenericRecord> genericFastClient =
        getGenericFastClient(clientConfigBuilder, new MetricsRepository(), fastD2Client);
    AvroGenericStoreClient<String, GenericRecord> grpcFastClient =
        getGenericFastClient(grpcClientConfigBuilder, new MetricsRepository(), grpcD2Client);

    Set<Set<String>> keySets = getKeySets();
    for (Set<String> keySet: keySets) {
      Map<String, GenericRecord> fastClientRet = genericFastClient.batchGet(keySet).get();
      Map<String, GenericRecord> grpcClientRet = grpcFastClient.batchGet(keySet).get();

      for (String key: keySet) {
        LOGGER.info("key: {}, fastClientRet: {}", key, fastClientRet.get(key));
        LOGGER.info("key: {}, grpcClientRet: {}", key, grpcClientRet.get(key));
        Assert.assertEquals(fastClientRet.get(key), grpcClientRet.get(key));
      }
    }
  }

  private Set<Set<String>> getKeySets() {
    Set<Set<String>> keySets = new HashSet<>();
    int numSets = recordCnt / maxAllowedKeys;
    int remainder = recordCnt % maxAllowedKeys;

    for (int i = 0; i < numSets; i++) {
      Set<String> keys = new HashSet<>();

      for (int j = 1; j <= maxAllowedKeys; j++) {
        keys.add(Integer.toString(i * maxAllowedKeys + j));
      }
      keySets.add(keys);
    }

    if (remainder > 0) {
      Set<String> keys = new HashSet<>();

      for (int j = 1; j <= remainder; j++) {
        keys.add(Integer.toString(numSets * maxAllowedKeys + j));
      }
      keySets.add(keys);
    }

    return keySets;
  }
}
