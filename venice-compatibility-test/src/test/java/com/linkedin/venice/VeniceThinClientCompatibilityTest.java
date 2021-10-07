package com.linkedin.venice;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.exceptions.VeniceException;
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
import java.util.concurrent.TimeoutException;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.venice.VeniceClusterInitializer.*;


/**
 * This test class is used to validate whether thin client could work with different Avro versions.
 * The high-level idea is as follows:
 * 1. Spin up a separate process, which will always use avro-1.7 to spin up a Venice Cluster and materialize
 *    a store and push some synthetic data into it.
 * 2. The test cases will use Venice-thin-client to hit the Router endpoint with different Avro Versions by
 *    exercising all different APIs.
 */
public class VeniceThinClientCompatibilityTest {
  private static final Logger LOGGER = Logger.getLogger(VeniceThinClientCompatibilityTest.class);

  private ForkedJavaProcess process;
  private String routerAddr;
  private AvroGenericStoreClient<String, GenericRecord> storeClient;
  private String storeName = TestUtils.getUniqueString("venice-store");

  @BeforeClass(alwaysRun = true)
  private void setup() {
    LOGGER.info("Avro version in unit test: " + AvroCompatibilityHelper.getRuntimeAvroVersion());
    /**
     * The following port selection is not super safe since this port could be occupied when it is being used
     * by the {@link VeniceClusterInitializer}.
     *
     * If it is flaky, maybe we could add some retry logic to make it more resilient in the future.
     */
    String routerPort = Integer.toString(Utils.getFreePort());
    routerAddr = "http://localhost:" + routerPort;
    try {
      process = ForkedJavaProcess.exec(VeniceClusterInitializer.class, Arrays.asList(storeName, routerPort), Collections.emptyList(),
          Optional.empty(), false, Optional.of(new ClassPathSupplierForVeniceCluster()));
    } catch (Exception e) {
      throw new VeniceException("Received an exception when forking a process", e);
    }

    // Block until the store is ready.
    Utils.sleep(TimeUnit.SECONDS.toMillis(10));
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      try {
        LOGGER.info("router addr in unit test: " + routerAddr);
        this.storeClient = ClientFactory.getAndStartGenericAvroClient(
            ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(routerAddr));
      } catch (Exception e) {
        Assert.fail("Store client is not ready yet.");
      }
        });
    String testKey = KEY_PREFIX + "0";
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      GenericRecord value = null;
      try {
        value = storeClient.get(testKey).get();
      } catch (Exception e) {}
      Assert.assertNotNull(value);
    });
  }

  @AfterClass(alwaysRun = true)
  private void cleanup() throws InterruptedException {
    if (storeClient != null) {
      Utils.closeQuietlyWithErrorLogged(storeClient);
    }
    if (process != null) {
      process.destroy();
      process.waitFor();
    }
  }

  @Test
  public void testSingleGet() throws ExecutionException, InterruptedException {
    String testKey = KEY_PREFIX + "1";
    GenericRecord value = storeClient.get(testKey).get();
    Assert.assertEquals(value.get("id").toString(), ID_FIELD_PREFIX + "1");
  }

  @Test
  public void testBatchGet() throws ExecutionException, InterruptedException {
    Set<String> keySet = new HashSet<>();
    int keyCount = 10;
    for (int i = 0; i < keyCount; ++i) {
      keySet.add(KEY_PREFIX + i);
    }
    Map<String, GenericRecord> resultMap = storeClient.batchGet(keySet).get();
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
    return Integer.valueOf(key.substring(keyPrefix.length()));
  }

  @Test
  public void testReadCompute() throws InterruptedException, ExecutionException, TimeoutException {
    int keyCount = 10;
    Set<String> keySet = new HashSet<>();
    for (int i = 0; i < keyCount; ++i) {
      keySet.add(KEY_PREFIX + i);
    }
    keySet.add("unknown_key");
    List<Float> p = Arrays.asList(100.0f, 0.1f);
    List<Float> cosP = Arrays.asList(123.4f, 5.6f);
    List<Float> hadamardP = Arrays.asList(135.7f, 246.8f);
    Map<String, GenericRecord> computeResult = storeClient.compute()
        .project("id")
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

    for (Map.Entry<String, GenericRecord> entry : computeResult.entrySet()) {
      int keyIdx = getKeyIndex(entry.getKey(), KEY_PREFIX);
      // check projection result
      Assert.assertEquals(entry.getValue().get("id"), new Utf8(ID_FIELD_PREFIX + keyIdx));
      // check dotProduct result; should be double for V1 request
      Assert.assertEquals(entry.getValue().get("member_score"), (float)(p.get(0) * (keyIdx + 1) + p.get(1) * ((keyIdx + 1) * 10)));

      // check cosine similarity result; should be double for V1 request
      float dotProductResult = (float) cosP.get(0) * (float)(keyIdx + 1) + cosP.get(1) * (float)((keyIdx + 1) * 10);
      float valueVectorMagnitude = (float) Math.sqrt(((float)(keyIdx + 1) * (float)(keyIdx + 1) + ((float)(keyIdx + 1) * 10.0f) * ((float)(keyIdx + 1) * 10.0f)));
      float parameterVectorMagnitude = (float) Math.sqrt((float)(cosP.get(0) * cosP.get(0) + cosP.get(1) * cosP.get(1)));
      float expectedCosineSimilarity = dotProductResult / (parameterVectorMagnitude * valueVectorMagnitude);
      Assert.assertEquals((float) entry.getValue().get("cosine_similarity_result"), expectedCosineSimilarity, 0.000001f);
      Assert.assertEquals((int) entry.getValue().get("member_feature_count"),  2);
      Assert.assertEquals((int) entry.getValue().get("namemap_count"),  0);

      // check hadamard product
      List<Float> hadamardProductResult = new ArrayList<>(2);
      hadamardProductResult.add(hadamardP.get(0) * (float)(keyIdx + 1));
      hadamardProductResult.add(hadamardP.get(1) * (float)((keyIdx + 1) * 10));
      Object hadamardProductResultReturned = entry.getValue().get("hadamard_product_result");
      Assert.assertTrue(hadamardProductResultReturned instanceof List);
      List hadamardProductResultReturnedList = (List)hadamardProductResultReturned;
      for (int i = 0; i < hadamardProductResultReturnedList.size(); ++i) {
        Assert.assertEquals(hadamardProductResultReturnedList.get(i), hadamardProductResult.get(i));
      }
    }
  }
}
