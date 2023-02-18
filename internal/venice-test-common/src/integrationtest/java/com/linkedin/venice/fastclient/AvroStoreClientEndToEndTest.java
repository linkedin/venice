package com.linkedin.venice.fastclient;

import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.fastclient.schema.TestValueSchema;
import com.linkedin.venice.fastclient.utils.AbstractClientEndToEndSetup;
import com.linkedin.venice.fastclient.utils.ClientTestUtils;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;


/**
 *
 * This class covers tests for:
 * 1. single get
 * 2. Batch get
 *
 * TODO
 * 1. There might be some duplicate tests in this file and {@link BatchGetAvroStoreClientTest}, need to clean it up.
 * 2. add test for get with speculative query but only with 1 replica
 */

public class AvroStoreClientEndToEndTest extends AbstractClientEndToEndSetup {
  protected void runTest(
      ClientConfig.ClientConfigBuilder clientConfigBuilder,
      boolean useDaVinciClientBasedMetadata,
      boolean batchGet,
      int batchGetKeySize) throws Exception {
    runTest(clientConfigBuilder, useDaVinciClientBasedMetadata, batchGet, batchGetKeySize, (metricsRepository) -> {});
  }

  /**
   * Run fast client tests based on the input parameters.
   * Only RouterBasedStoreMetadata can be reused. Other StoreMetadata implementation cannot be used after close() is called.
   *
   * @param clientConfigBuilder config to build client
   * @param useDaVinciClientBasedMetadata true: use DaVinci based meta data, false: use Router based meta data
   * @param batchGet singleGet or batchGet
   * @throws Exception
   */
  protected void runTest(
      ClientConfig.ClientConfigBuilder clientConfigBuilder,
      boolean useDaVinciClientBasedMetadata,
      boolean batchGet,
      int batchGetKeySize,
      Consumer<MetricsRepository> statsValidation) throws Exception {
    MetricsRepository metricsRepositoryForGenericClient = new MetricsRepository();
    AvroGenericStoreClient<String, GenericRecord> genericFastClient =
        getGenericFastClient(clientConfigBuilder, metricsRepositoryForGenericClient, useDaVinciClientBasedMetadata);

    AvroGenericStoreClient<String, Object> genericFastVsonClient = null;
    // Construct a Vson store client
    genericFastVsonClient =
        getGenericFastVsonClient(clientConfigBuilder.clone(), new MetricsRepository(), useDaVinciClientBasedMetadata);
    try {
      if (batchGet) {
        // test batch get of size 2 (current default max)
        if (batchGetKeySize == 2) {
          for (int i = 0; i < recordCnt - 1; ++i) {
            String key1 = keyPrefix + i;
            String key2 = keyPrefix + (i + 1);
            Set<String> keys = new HashSet<>();
            keys.add(key1);
            keys.add(key2);
            Map<String, GenericRecord> resultMap = genericFastClient.batchGet(keys).get();
            assertEquals(resultMap.size(), 2);
            assertEquals((int) resultMap.get(key1).get(VALUE_FIELD_NAME), i);
            assertEquals((int) resultMap.get(key2).get(VALUE_FIELD_NAME), i + 1);

            // Test Vson client
            Map<String, Object> vsonResultMapofObj = genericFastVsonClient.batchGet(keys).get();
            assertEquals(vsonResultMapofObj.size(), 2);

            Object vsonResultObj = vsonResultMapofObj.get(key1);
            assertTrue(
                vsonResultObj instanceof Map,
                "VsonClient should return Map, but got" + vsonResultObj.getClass());
            Map vsonResult = (Map) vsonResultObj;
            assertEquals((int) vsonResult.get(VALUE_FIELD_NAME), i);

            vsonResultObj = vsonResultMapofObj.get(key2);
            assertTrue(
                vsonResultObj instanceof Map,
                "VsonClient should return Map, but got" + vsonResultObj.getClass());
            vsonResult = (Map) vsonResultObj;
            assertEquals((int) vsonResult.get(VALUE_FIELD_NAME), i + 1);
          }
        } else if (batchGetKeySize == recordCnt) {
          // test batch get of size recordCnt (configured)
          Set<String> keys = new HashSet<>();
          for (int i = 0; i < recordCnt; ++i) {
            String key = keyPrefix + i;
            keys.add(key);
          }
          Map<String, GenericRecord> resultMap = genericFastClient.batchGet(keys).get();
          assertEquals(resultMap.size(), recordCnt);

          for (int i = 0; i < recordCnt; ++i) {
            String key = keyPrefix + i;
            assertEquals((int) resultMap.get(key).get(VALUE_FIELD_NAME), i);
          }

          // vson
          Map<String, Object> vsonResultMapofObj = genericFastVsonClient.batchGet(keys).get();
          assertEquals(vsonResultMapofObj.size(), recordCnt);

          for (int i = 0; i < recordCnt; ++i) {
            String key = keyPrefix + i;
            Object vsonResultObj = vsonResultMapofObj.get(key);
            assertTrue(
                vsonResultObj instanceof Map,
                "VsonClient should return Map, but got" + vsonResultObj.getClass());
            Map vsonResult = (Map) vsonResultObj;
            assertEquals((int) vsonResult.get(VALUE_FIELD_NAME), i);
          }
        } else {
          throw new VeniceException("unsupported batchGetKeySize: " + batchGetKeySize);
        }
      } else {
        for (int i = 0; i < recordCnt; ++i) {
          String key = keyPrefix + i;
          GenericRecord value = genericFastClient.get(key).get();
          assertEquals((int) value.get(VALUE_FIELD_NAME), i);

          // Test Vson client
          Object vsonResult = genericFastVsonClient.get(key).get();
          assertTrue(vsonResult instanceof Map, "VsonClient should return Map, but got" + vsonResult.getClass());
          Map vsonValue = (Map) vsonResult;
          assertEquals((int) vsonValue.get(VALUE_FIELD_NAME), i);
        }
      }
      statsValidation.accept(metricsRepositoryForGenericClient);
    } finally {
      if (genericFastClient != null) {
        genericFastClient.close();
      }
      if (genericFastVsonClient != null) {
        genericFastVsonClient.close();
      }
      cleanupDaVinciClientForMetaStore();
    }

    // Test specific store client
    MetricsRepository metricsRepositoryForSpecificClient = new MetricsRepository();
    ClientConfig.ClientConfigBuilder specificClientConfigBuilder = clientConfigBuilder.clone();
    AvroSpecificStoreClient<String, TestValueSchema> specificFastClient = getSpecificFastClient(
        specificClientConfigBuilder,
        metricsRepositoryForSpecificClient,
        useDaVinciClientBasedMetadata,
        TestValueSchema.class);
    try {
      if (batchGet) {
        // test batch get of size 2 (default)
        if (batchGetKeySize == 2) {
          for (int i = 0; i < recordCnt - 1; ++i) {
            String key1 = keyPrefix + i;
            String key2 = keyPrefix + (i + 1);
            Set<String> keys = new HashSet<>();
            keys.add(key1);
            keys.add(key2);
            Map<String, TestValueSchema> resultMap = specificFastClient.batchGet(keys).get();
            assertEquals(resultMap.size(), 2);
            assertEquals(resultMap.get(key1).int_field, i);
            assertEquals(resultMap.get(key2).int_field, i + 1);
          }
        } else if (batchGetKeySize == recordCnt) {
          // test batch get of size recordCnt (configured)
          Set<String> keys = new HashSet<>();
          for (int i = 0; i < recordCnt; ++i) {
            String key = keyPrefix + i;
            keys.add(key);
          }
          Map<String, TestValueSchema> resultMap = specificFastClient.batchGet(keys).get();
          assertEquals(resultMap.size(), recordCnt);

          for (int i = 0; i < recordCnt; ++i) {
            String key = keyPrefix + i;
            assertEquals(resultMap.get(key).int_field, i);
          }
        } else {
          throw new VeniceException("unsupported batchGetKeySize: " + batchGetKeySize);
        }
      } else {
        for (int i = 0; i < recordCnt; ++i) {
          String key = keyPrefix + i;
          TestValueSchema value = specificFastClient.get(key).get();
          assertEquals(value.int_field, i);
        }
      }
      statsValidation.accept(metricsRepositoryForSpecificClient);
    } finally {
      if (specificFastClient != null) {
        specificFastClient.close();
      }
      cleanupDaVinciClientForMetaStore();
    }
  }

  @Test(dataProvider = "FastClient-Four-Boolean-And-A-Number", timeOut = TIME_OUT)
  public void testFastClientGet(
      boolean useDaVinciClientBasedMetadata,
      boolean multiGet,
      boolean dualRead,
      boolean speculativeQueryEnabled,
      int batchGetKeySize) throws Exception {
    if (multiGet == false && batchGetKeySize != (int) BATCH_GET_KEY_SIZE[0]) {
      // redundant case as batchGetKeySize doesn't apply for single gets, so run only once
      // TODO add a better dataProvider
      return;
    }
    ClientConfig.ClientConfigBuilder clientConfigBuilder =
        new ClientConfig.ClientConfigBuilder<>().setStoreName(storeName)
            .setR2Client(r2Client)
            .setSpeculativeQueryEnabled(speculativeQueryEnabled)
            .setDualReadEnabled(dualRead)
            // default maxAllowedKeyCntInBatchGetReq is 2. configuring it to test different cases.
            .setMaxAllowedKeyCntInBatchGetReq(recordCnt);

    // dualRead also needs thinClient
    AvroGenericStoreClient<String, GenericRecord> genericThinClient = null;
    AvroSpecificStoreClient<String, TestValueSchema> specificThinClient = null;

    try {
      if (dualRead) {
        genericThinClient = getGenericThinClient();
        clientConfigBuilder.setGenericThinClient(genericThinClient);
        specificThinClient = getSpecificThinClient();
        clientConfigBuilder.setSpecificThinClient(specificThinClient);
      }

      runTest(clientConfigBuilder, useDaVinciClientBasedMetadata, multiGet, batchGetKeySize);
    } finally {
      if (genericThinClient != null) {
        genericThinClient.close();
      }
      if (specificThinClient != null) {
        specificThinClient.close();
      }
    }
  }

  @Test(dataProvider = "fastClientHTTPVariants", dataProviderClass = ClientTestUtils.class, timeOut = TIME_OUT)
  public void testFastClientGetWithDifferentHTTPVariants(ClientTestUtils.FastClientHTTPVariant fastClientHTTPVariant)
      throws Exception {
    boolean useDaVinciClientBasedMetadata = true;
    Client r2Client = ClientTestUtils.getR2Client(fastClientHTTPVariant);
    ClientConfig.ClientConfigBuilder clientConfigBuilder =
        new ClientConfig.ClientConfigBuilder<>().setStoreName(storeName)
            .setR2Client(r2Client)
            .setDualReadEnabled(false);

    // test both single and multiGet
    runTest(clientConfigBuilder, useDaVinciClientBasedMetadata, false, 2);
    runTest(clientConfigBuilder, useDaVinciClientBasedMetadata, true, 2);
  }

  // TODO This test fails for the first time and then succeeds when the entire fastclient
  // testsuite is run, but runs successfully when this test is run alone. Need to debug.
  @Test
  public void testFastClientSingleGetLongTailRetry() throws Exception {
    ClientConfig.ClientConfigBuilder clientConfigBuilder =
        new ClientConfig.ClientConfigBuilder<>().setStoreName(storeName)
            .setR2Client(r2Client)
            .setSpeculativeQueryEnabled(false)
            .setLongTailRetryEnabledForSingleGet(true)
            .setLongTailRetryThresholdForSingleGetInMicroSeconds(10); // Try to trigger long-tail retry as much as
                                                                      // possible.

    runTest(clientConfigBuilder, true, false, 2, metricsRepository -> {
      // Validate long-tail retry related metrics
      metricsRepository.metrics().forEach((mName, metric) -> {
        if (mName.contains("--long_tail_retry_request.OccurrenceRate")) {
          assertTrue(metric.value() > 0, "Long tail retry for single-get should be triggered");
        }
      });
    });
  }
}
