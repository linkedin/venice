package com.linkedin.venice;

import static com.linkedin.venice.VeniceClusterInitializer.ID_FIELD_PREFIX;
import static com.linkedin.venice.VeniceClusterInitializer.KEY_PREFIX;
import static com.linkedin.venice.VeniceClusterInitializer.ZK_ADDRESS_FIELD;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelperCommon;
import com.linkedin.avroutil1.compatibility.AvroVersion;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.client.store.ComputeGenericRecord;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.utils.ClassPathSupplierForVeniceCluster;
import com.linkedin.venice.utils.ForkedJavaProcess;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * This test class is used to validate whether thin client could work with different Avro versions.
 * The high-level idea is as follows:
 * 1. Spin up a separate process, which will always use avro-1.9 to spin up a Venice Cluster and materialize
 *    a store and push some synthetic data into it.
 * 2. The test cases will use Venice-thin-client to hit the Router endpoint with different Avro Versions by
 *    exercising all different APIs.
 */
public class VeniceClientCompatibilityTest {
  private static final Logger LOGGER = LogManager.getLogger(VeniceClientCompatibilityTest.class);

  private ForkedJavaProcess clusterProcess;
  private AvroGenericStoreClient<String, GenericRecord> veniceClient;
  private DaVinciClient<String, GenericRecord> daVinciClient;

  @BeforeClass
  public void setUp() throws Exception {
    LOGGER.info("Avro version in unit test: {}", AvroCompatibilityHelperCommon.getRuntimeAvroVersion());
    Assert.assertEquals(
        AvroCompatibilityHelperCommon.getRuntimeAvroVersion(),
        AvroVersion.valueOf(System.getProperty("clientAvroVersion")));
    /**
     * The following port selection is not super safe since this port could be occupied when it is being used
     * by the {@link VeniceClusterInitializer}.
     *
     * If it is flaky, maybe we could add some retry logic to make it more resilient in the future.
     */
    String routerPort = Integer.toString(TestUtils.getFreePort());
    String routerAddress = "http://localhost:" + routerPort;
    LOGGER.info("Router address in unit test: {}", routerAddress);

    String storeName = Utils.getUniqueString("venice-store");
    clusterProcess = ForkedJavaProcess.exec(
        VeniceClusterInitializer.class,
        Arrays.asList(storeName, routerPort),
        Collections.emptyList(),
        new ClassPathSupplierForVeniceCluster().get(),
        true,
        Optional.empty());

    veniceClient = ClientFactory.getGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName)
            .setVeniceURL(routerAddress)
            .setForceClusterDiscoveryAtStartTime(true));

    // Block until the store is ready.
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
      if (!clusterProcess.isAlive()) {
        throw new VeniceException("Cluster process exited unexpectedly.");
      }
      Assert.assertTrue(clusterProcess.isAlive());
      try {
        veniceClient.start();
      } catch (VeniceException e) {
        Assert.fail("Store is not ready yet.", e);
      }
    });

    String[] zkAddress = new String[1];
    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, () -> {
      if (!clusterProcess.isAlive()) {
        throw new VeniceException("Cluster process exited unexpectedly.");
      }
      try {
        GenericRecord value = veniceClient.get(KEY_PREFIX + "0").get();
        Assert.assertNotNull(value);
        zkAddress[0] = value.get(ZK_ADDRESS_FIELD).toString();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (VeniceException | ExecutionException e) {
        Assert.fail("Failed to query test key.", e);
      }
    });
    Assert.assertNotNull(zkAddress[0]);
    LOGGER.info("Zookeeper address in unit test: {}", zkAddress[0]);

    daVinciClient = ServiceFactory.getGenericAvroDaVinciClientWithoutMetaSystemStoreRepo(
        storeName,
        zkAddress[0],
        Utils.getTempDataDirectory().getAbsolutePath());
    daVinciClient.subscribeAll().get(60, TimeUnit.SECONDS);
  }

  @AfterClass
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(veniceClient);
    Utils.closeQuietlyWithErrorLogged(daVinciClient);
    if (clusterProcess != null) {
      clusterProcess.destroy();
    }
  }

  @DataProvider(name = "clientProvider")
  public Object[][] clientProvider() {
    return new Object[][] { { veniceClient }, { daVinciClient } };
  }

  @Test(dataProvider = "clientProvider")
  public void testSingleGet(AvroGenericStoreClient<String, GenericRecord> client) throws Exception {
    String testKey = KEY_PREFIX + "1";
    GenericRecord value = client.get(testKey).get();
    Assert.assertEquals(value.get("id").toString(), ID_FIELD_PREFIX + "1");
  }

  @Test(dataProvider = "clientProvider")
  public void testBatchGet(AvroGenericStoreClient<String, GenericRecord> client) throws Exception {
    Set<String> keySet = new HashSet<>();
    int keyCount = 10;
    for (int i = 0; i < keyCount; ++i) {
      keySet.add(KEY_PREFIX + i);
    }
    Map<String, GenericRecord> resultMap = client.batchGet(keySet).get();
    Assert.assertEquals(resultMap.size(), keyCount);
    for (int i = 0; i < keyCount; ++i) {
      GenericRecord value = resultMap.get(KEY_PREFIX + i);
      Assert.assertEquals(value.get("id").toString(), ID_FIELD_PREFIX + i);
    }
  }

  private int getKeyIndex(String key, String keyPrefix) {
    if (!key.startsWith(keyPrefix)) {
      return -1;
    }
    return Integer.parseInt(key.substring(keyPrefix.length()));
  }

  @Test(dataProvider = "clientProvider")
  public void testReadCompute(AvroGenericStoreClient<String, GenericRecord> client) throws Exception {
    int keyCount = 10;
    Set<String> keySet = new HashSet<>();
    for (int i = 0; i < keyCount; ++i) {
      keySet.add(KEY_PREFIX + i);
    }
    keySet.add("unknown_key");
    List<Float> p = Arrays.asList(100.0f, 0.1f);
    List<Float> cosP = Arrays.asList(123.4f, 5.6f);
    List<Float> hadamardP = Arrays.asList(135.7f, 246.8f);
    Map<String, ComputeGenericRecord> computeResult = client.compute()
        .project("id", "boolean_field", "int_field", "float_field", "member_feature")
        .dotProduct("member_feature", p, "member_score")
        .cosineSimilarity("member_feature", cosP, "cosine_similarity_result")
        .hadamardProduct("member_feature", hadamardP, "hadamard_product_result")
        .count("namemap", "namemap_count")
        .count("member_feature", "member_feature_count")
        .execute(keySet)
        /**
         * Added 2s timeout as a safety net as ideally each request should take sub-second.
         */
        .get(2, TimeUnit.SECONDS);
    Assert.assertEquals(computeResult.size(), keyCount);

    for (Map.Entry<String, ComputeGenericRecord> entry: computeResult.entrySet()) {
      int keyIdx = getKeyIndex(entry.getKey(), KEY_PREFIX);
      // check projection result
      Assert.assertEquals(entry.getValue().get("id"), new Utf8(ID_FIELD_PREFIX + keyIdx));
      // check dotProduct result; should be double for V1 request
      Assert.assertEquals(
          entry.getValue().get("member_score"),
          (float) (p.get(0) * (keyIdx + 1) + p.get(1) * ((keyIdx + 1) * 10)));

      // check cosine similarity result; should be double for V1 request
      float dotProductResult = (float) cosP.get(0) * (float) (keyIdx + 1) + cosP.get(1) * (float) ((keyIdx + 1) * 10);
      float valueVectorMagnitude = (float) Math.sqrt(
          ((float) (keyIdx + 1) * (float) (keyIdx + 1)
              + ((float) (keyIdx + 1) * 10.0f) * ((float) (keyIdx + 1) * 10.0f)));
      float parameterVectorMagnitude =
          (float) Math.sqrt((float) (cosP.get(0) * cosP.get(0) + cosP.get(1) * cosP.get(1)));
      float expectedCosineSimilarity = dotProductResult / (parameterVectorMagnitude * valueVectorMagnitude);
      Assert
          .assertEquals((float) entry.getValue().get("cosine_similarity_result"), expectedCosineSimilarity, 0.000001f);
      Assert.assertEquals((int) entry.getValue().get("member_feature_count"), 2);
      Assert.assertEquals((int) entry.getValue().get("namemap_count"), 0);

      // check hadamard product
      List<Float> hadamardProductResult = new ArrayList<>(2);
      hadamardProductResult.add(hadamardP.get(0) * (float) (keyIdx + 1));
      hadamardProductResult.add(hadamardP.get(1) * (float) ((keyIdx + 1) * 10));
      Object hadamardProductResultReturned = entry.getValue().get("hadamard_product_result");
      Assert.assertTrue(hadamardProductResultReturned instanceof List);
      List hadamardProductResultReturnedList = (List) hadamardProductResultReturned;
      for (int i = 0; i < hadamardProductResultReturnedList.size(); ++i) {
        Assert.assertEquals(hadamardProductResultReturnedList.get(i), hadamardProductResult.get(i));
      }
    }
  }
}
