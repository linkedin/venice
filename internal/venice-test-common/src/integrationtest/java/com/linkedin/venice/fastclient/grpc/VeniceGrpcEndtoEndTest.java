package com.linkedin.venice.fastclient.grpc;

import static com.linkedin.venice.ConfigKeys.*;

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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class VeniceGrpcEndtoEndTest extends AbstractClientEndToEndSetup {
  private VeniceClusterWrapper cluster;

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

  public String writeSyntheticData(String storeName) throws IOException {
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
  public void testThinClient() throws Exception {
    String storeName = writeSyntheticData("thin-client-store");

    // 4. Create thin client and read data from cluster/store
    try (AvroGenericStoreClient<Object, Object> avroClient = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(cluster.getRandomRouterURL()))) {

      for (int i = 1; i <= 100; i++) {
        Object ret = avroClient.get(Integer.toString(i)).get();
        Assert.assertEquals(ret.toString(), "test_name_" + i);
      }
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testFastClient() throws Exception {
    String storeName = writeSyntheticData("fast-client-store");

    // 4. Create fast client and read data from cluster/store
    r2Client = ClientTestUtils.getR2Client(ClientTestUtils.FastClientHTTPVariant.HTTP_2_BASED_R2_CLIENT);
    d2Client = D2TestUtils.getAndStartHttpsD2Client(cluster.getZk().getAddress());

    com.linkedin.venice.fastclient.ClientConfig.ClientConfigBuilder<Object, Object, SpecificRecord> clientConfigBuilder =
        new com.linkedin.venice.fastclient.ClientConfig.ClientConfigBuilder<>().setStoreName(storeName)
            .setR2Client(r2Client)
            .setMaxAllowedKeyCntInBatchGetReq(150)
            .setSpeculativeQueryEnabled(false);

    AvroGenericStoreClient<String, GenericRecord> genericFastClient = getGenericFastClient(
        clientConfigBuilder,
        new MetricsRepository(),
        StoreMetadataFetchMode.SERVER_BASED_METADATA);

    List<Set<String>> keys = new ArrayList<>();
    for (int i = 1; i < 100; i++) {
      Set<String> keySet = new HashSet<>();
      keySet.add(Integer.toString(i));
      keySet.add(Integer.toString(i + 1));
      keys.add(keySet);
    }

    try {
      for (Set<String> key: keys) {
        Map<String, GenericRecord> ret = genericFastClient.batchGet(key).get();
        for (Map.Entry<String, GenericRecord> entry: ret.entrySet()) {
          Assert.assertEquals(entry.toString(), entry.getKey() + "=test_name_" + entry.getKey());
        }
      }
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
