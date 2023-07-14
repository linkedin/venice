package com.linkedin.venice.fastclient.grpc;

import static com.linkedin.venice.ConfigKeys.CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE;
import static com.linkedin.venice.ConfigKeys.ENABLE_GRPC_READ_SERVER;
import static com.linkedin.venice.ConfigKeys.PARTICIPANT_MESSAGE_STORE_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_HTTP2_INBOUND_ENABLED;
import static com.linkedin.venice.ConfigKeys.SERVER_QUOTA_ENFORCEMENT_ENABLED;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerResponse;
import com.linkedin.venice.controllerapi.NewStoreResponse;
import com.linkedin.venice.controllerapi.StoreResponse;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.fastclient.meta.StoreMetadataFetchMode;
import com.linkedin.venice.fastclient.utils.AbstractClientEndToEndSetup;
import com.linkedin.venice.fastclient.utils.ClientTestUtils;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.listener.grpc.VeniceReadServiceClient;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import io.tehuti.metrics.MetricsRepository;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.util.Utf8;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class VeniceGrpcEndToEndTest extends AbstractClientEndToEndSetup {
  private VeniceClusterWrapper cluster;

  @BeforeClass
  public void setUp() throws Exception {
    Utils.thisIsLocalhost();

    Properties props = new Properties();
    props.setProperty(PARTICIPANT_MESSAGE_STORE_ENABLED, "false");
    props.setProperty(CONTROLLER_AUTO_MATERIALIZE_META_SYSTEM_STORE, "true");
    props.put(SERVER_HTTP2_INBOUND_ENABLED, "true");
    props.put(SERVER_QUOTA_ENFORCEMENT_ENABLED, "true");
    props.put(ENABLE_GRPC_READ_SERVER, "true");

    cluster = ServiceFactory.getVeniceCluster(
        new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
            .numberOfPartitions(1)
            .numberOfRouters(1)
            .numberOfServers(1)
            .sslToStorageNodes(true)
            .extraProperties(props)
            .build());
  }

  @AfterClass
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(cluster);
  }

  public String writeData(String storeName) throws IOException {
    // 1. Create a new store in Venice
    String uniqueStoreName = Utils.getUniqueString(storeName);
    NewStoreResponse response = cluster.getNewStore(uniqueStoreName);
    UpdateStoreQueryParams params = new UpdateStoreQueryParams().setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA);

    Assert.assertEquals(response.getName(), uniqueStoreName);
    Assert.assertNull(response.getError());

    ControllerResponse updateStoreResponse = cluster.updateStore(uniqueStoreName, params);
    Assert.assertNull(updateStoreResponse.getError());

    // 2. Write Synthetic data to the store w/ writeSimpleAvroFileWithUserSchema
    File inputDir = TestWriteUtils.getTempDataDirectory();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    TestWriteUtils.writeSimpleAvroFileWithUserSchema(inputDir);

    // 3. Run a push job to push the data to Venice (VPJ)
    Properties vpjProps = TestWriteUtils.defaultVPJProps(cluster.getRandomRouterURL(), inputDirPath, uniqueStoreName);
    TestWriteUtils.runPushJob("test push job", vpjProps);

    cluster.useControllerClient(
        controllerClient -> TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
          StoreResponse storeResponse = controllerClient.getStore(uniqueStoreName);
          Assert.assertEquals(storeResponse.getStore().getCurrentVersion(), 1);
        }));

    return uniqueStoreName;
  }

  @Test
  public void testReadData() throws Exception {
    String storeName = writeData("new-store");

    // 4. Create thin client
    AvroGenericStoreClient<Object, Object> avroClient = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(cluster.getRandomRouterURL()));

    // 4. Create fastClient
    r2Client = ClientTestUtils.getR2Client(ClientTestUtils.FastClientHTTPVariant.HTTP_2_BASED_R2_CLIENT);
    d2Client = D2TestUtils.getAndStartHttpsD2Client(cluster.getZk().getAddress());

    com.linkedin.venice.fastclient.ClientConfig.ClientConfigBuilder<Object, Object, SpecificRecord> clientConfigBuilder =
        new com.linkedin.venice.fastclient.ClientConfig.ClientConfigBuilder<>().setStoreName(storeName)
            .setR2Client(r2Client)
            .setMaxAllowedKeyCntInBatchGetReq(2)
            .setSpeculativeQueryEnabled(false);

    AvroGenericStoreClient<String, GenericRecord> genericFastClient = getGenericFastClient(
        clientConfigBuilder,
        new MetricsRepository(),
        StoreMetadataFetchMode.SERVER_BASED_METADATA);

    VeniceReadServiceClient grpcReadClient = new VeniceReadServiceClient("localhost:8080");
    String respA = grpcReadClient.get("this is a test key");
    String respB = grpcReadClient.get("this is another test key");

    Assert.assertEquals(respA, "this is a test keythis is a test key");
    Assert.assertEquals(respB, "this is another test keythis is another test key");

    // generate list of keys
    List<Set<String>> keySets = new ArrayList<>();
    for (int i = 1; i <= 100; i++) {
      Set<String> key = new HashSet<>();
      key.add(Integer.toString(i));
      keySets.add(key);
    }

    // iterate through list and obtain records through both clients
    for (Set<String> key: keySets) {
      Map<String, GenericRecord> fastClientRet = genericFastClient.batchGet(key).get();
      for (String k: key) {
        String fastClientRetRecord = ((Utf8) fastClientRet.get(k)).toString();
        String thinClientRecord = avroClient.get(k).get().toString();
        String grpcClientRecord = grpcReadClient.get(fastClientRetRecord);

        Assert.assertEquals(grpcClientRecord, fastClientRetRecord + thinClientRecord);
        Assert.assertEquals(fastClientRetRecord, thinClientRecord);
      }
    }
  }
}
