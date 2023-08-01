package com.linkedin.venice.fastclient;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.fastclient.meta.StoreMetadataFetchMode;
import com.linkedin.venice.fastclient.schema.TestValueSchema;
import com.linkedin.venice.fastclient.utils.AbstractClientEndToEndSetup;
import com.linkedin.venice.fastclient.utils.ClientTestUtils;
import com.linkedin.venice.integration.utils.VeniceServerWrapper;
import com.linkedin.venice.utils.TestUtils;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
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
      boolean batchGet,
      int batchGetKeySize,
      Optional<AvroGenericStoreClient> vsonThinClient,
      StoreMetadataFetchMode storeMetadataFetchMode) throws Exception {
    runTest(
        clientConfigBuilder,
        batchGet,
        batchGetKeySize,
        (metricsRepository) -> {},
        (metricsRepository) -> {},
        null,
        vsonThinClient,
        storeMetadataFetchMode);
  }

  /**
   * Run fast client tests based on the input parameters.
   * Only RouterBasedStoreMetadata can be reused. Other StoreMetadata implementation cannot be used after close() is called.
   *
   * @param clientConfigBuilder config to build client
   * @param batchGet singleGet or batchGet
   * @throws Exception
   */
  protected void runTest(
      ClientConfig.ClientConfigBuilder clientConfigBuilder,
      boolean batchGet,
      int batchGetKeySize,
      Consumer<MetricsRepository> fastClientStatsValidation,
      Consumer<MetricsRepository> thinClientStatsValidation,
      MetricsRepository thinClientMetricsRepository,
      Optional<AvroGenericStoreClient> vsonThinClient,
      StoreMetadataFetchMode storeMetadataFetchMode) throws Exception {
    MetricsRepository metricsRepositoryForGenericClient = new MetricsRepository();
    AvroGenericStoreClient<String, GenericRecord> genericFastClient = null;
    AvroGenericStoreClient<String, Object> genericFastVsonClient = null;
    try {
      genericFastClient =
          getGenericFastClient(clientConfigBuilder, metricsRepositoryForGenericClient, storeMetadataFetchMode);

      // Construct a Vson store client
      genericFastVsonClient = getGenericFastVsonClient(
          clientConfigBuilder.clone(),
          new MetricsRepository(),
          vsonThinClient,
          storeMetadataFetchMode);

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
                "VsonClient should return Map, but got " + vsonResultObj.getClass());
            Map vsonResult = (Map) vsonResultObj;
            assertEquals((int) vsonResult.get(VALUE_FIELD_NAME), i);

            vsonResultObj = vsonResultMapofObj.get(key2);
            assertTrue(
                vsonResultObj instanceof Map,
                "VsonClient should return Map, but got " + vsonResultObj.getClass());
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
      fastClientStatsValidation.accept(metricsRepositoryForGenericClient);
      thinClientStatsValidation.accept(thinClientMetricsRepository);
    } finally {
      if (genericFastClient != null) {
        genericFastClient.close();
      }
      if (genericFastVsonClient != null) {
        genericFastVsonClient.close();
      }
    }

    // Test specific store client
    MetricsRepository metricsRepositoryForSpecificClient = new MetricsRepository();
    ClientConfig.ClientConfigBuilder specificClientConfigBuilder = clientConfigBuilder.clone();
    AvroSpecificStoreClient<String, TestValueSchema> specificFastClient = getSpecificFastClient(
        specificClientConfigBuilder,
        metricsRepositoryForSpecificClient,
        TestValueSchema.class,
        storeMetadataFetchMode);
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
      fastClientStatsValidation.accept(metricsRepositoryForSpecificClient);
    } finally {
      if (specificFastClient != null) {
        specificFastClient.close();
      }
    }
  }

  @Test(dataProvider = "FastClient-Five-Boolean-A-Number-Store-Metadata-Fetch-Mode", timeOut = TIME_OUT)
  public void testFastClientGet(
      boolean dualRead,
      boolean speculativeQueryEnabled,
      boolean batchGet,
      boolean useStreamingBatchGetAsDefault,
      boolean enableGrpc,
      int batchGetKeySize,
      StoreMetadataFetchMode storeMetadataFetchMode) throws Exception {
    ClientConfig.ClientConfigBuilder clientConfigBuilder =
        new ClientConfig.ClientConfigBuilder<>().setStoreName(storeName)
            .setR2Client(r2Client)
            .setSpeculativeQueryEnabled(speculativeQueryEnabled)
            .setDualReadEnabled(dualRead)
            // this needs to be revisited to see how much this should be set. Current default is 50.
            .setRoutingPendingRequestCounterInstanceBlockThreshold(recordCnt);

    if (batchGet) {
      clientConfigBuilder
          // default maxAllowedKeyCntInBatchGetReq is 2. configuring it to test different cases.
          .setMaxAllowedKeyCntInBatchGetReq(recordCnt)
          .setUseStreamingBatchGetAsDefault(useStreamingBatchGetAsDefault);
    }

    if (enableGrpc) {
      clientConfigBuilder.setNettyServerToGrpcAddressMap(veniceCluster.getNettyToGrpcServerMap()).setUseGrpc(true);
    }

    // dualRead needs thinClient
    AvroGenericStoreClient<String, GenericRecord> genericThinClient = null;
    AvroSpecificStoreClient<String, TestValueSchema> specificThinClient = null;
    AvroGenericStoreClient<String, Object> genericVsonThinClient = null;
    MetricsRepository thinClientMetricsRepository = new MetricsRepository();
    Consumer<MetricsRepository> thinClientStatsValidation;

    try {
      if (dualRead) {
        genericThinClient = getGenericThinClient(thinClientMetricsRepository);
        clientConfigBuilder.setGenericThinClient(genericThinClient);
        specificThinClient = getSpecificThinClient();
        clientConfigBuilder.setSpecificThinClient(specificThinClient);
        genericVsonThinClient = getGenericVsonThinClient();
        thinClientStatsValidation = metricsRepository -> {
          TestUtils.waitForNonDeterministicAssertion(
              10,
              TimeUnit.SECONDS,
              () -> assertTrue(
                  metricsRepository.metrics()
                      .get("." + storeName + (batchGet ? "--multiget_streaming_" : "--") + "request_key_count.Rate")
                      .value() > 0,
                  "Thin client metrics should be incremented when dual read is enabled"));
        };
      } else {
        thinClientStatsValidation = metricsRepository -> {
          metricsRepository.metrics()
              .forEach(
                  (mName, metric) -> assertTrue(
                      metric.value() == 0,
                      "Thin client metrics should not be incremented when dual read is disabled"));
        };
      }

      Consumer<MetricsRepository> fastClientStatsValidation = metricsRepository -> validateMetrics(
          metricsRepository,
          useStreamingBatchGetAsDefault,
          batchGetKeySize,
          batchGetKeySize);

      runTest(
          clientConfigBuilder,
          batchGet,
          batchGetKeySize,
          fastClientStatsValidation,
          thinClientStatsValidation,
          thinClientMetricsRepository,
          dualRead ? Optional.of(genericVsonThinClient) : Optional.empty(),
          storeMetadataFetchMode);

    } finally {
      if (genericThinClient != null) {
        genericThinClient.close();
      }
      if (specificThinClient != null) {
        specificThinClient.close();
      }
      if (genericVsonThinClient != null) {
        genericVsonThinClient.close();
      }
    }
  }

  @Test(expectedExceptions = { VeniceClientException.class,
      ExecutionException.class }, expectedExceptionsMessageRegExp = ".* metadata is not ready, attempting to re-initialize", dataProvider = "FastClient-Three-Boolean-And-A-Number", timeOut = TIME_OUT)
  public void testFastClientWithoutServers(
      boolean multiGet,
      boolean dualRead,
      boolean speculativeQueryEnabled,
      int batchGetKeySize) throws Exception {
    // stop all servers
    for (VeniceServerWrapper veniceServerWrapper: veniceCluster.getVeniceServers()) {
      veniceCluster.stopVeniceServer(veniceServerWrapper.getPort());
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
    AvroGenericStoreClient<String, Object> genericVsonThinClient = null;
    MetricsRepository thinClientMetricsRepository = new MetricsRepository();

    try {
      if (dualRead) {
        genericThinClient = getGenericThinClient(thinClientMetricsRepository);
        clientConfigBuilder.setGenericThinClient(genericThinClient);
        specificThinClient = getSpecificThinClient();
        clientConfigBuilder.setSpecificThinClient(specificThinClient);
        genericVsonThinClient = getGenericVsonThinClient();
      }
      runTest(
          clientConfigBuilder,
          multiGet,
          batchGetKeySize,
          dualRead ? Optional.of(genericVsonThinClient) : Optional.empty(),
          StoreMetadataFetchMode.SERVER_BASED_METADATA);
    } finally {
      if (genericThinClient != null) {
        genericThinClient.close();
      }
      if (specificThinClient != null) {
        specificThinClient.close();
      }
      if (genericVsonThinClient != null) {
        genericVsonThinClient.close();
      }
    }
  }

  @Test(dataProvider = "fastClientHTTPVariantsAndStoreMetadataFetchModes", timeOut = TIME_OUT)
  public void testFastClientGetWithDifferentHTTPVariants(
      ClientTestUtils.FastClientHTTPVariant fastClientHTTPVariant,
      StoreMetadataFetchMode storeMetadataFetchMode) throws Exception {
    Client r2Client = ClientTestUtils.getR2Client(fastClientHTTPVariant);
    ClientConfig.ClientConfigBuilder clientConfigBuilder =
        new ClientConfig.ClientConfigBuilder<>().setStoreName(storeName)
            .setR2Client(r2Client)
            .setDualReadEnabled(false);

    Consumer<MetricsRepository> fastClientStatsValidation = metricsRepository -> {
      metricsRepository.metrics().forEach((mName, metric) -> {
        if (mName.contains("long_tail_retry_request")) {
          assertTrue(metric.value() == 0, "Long tail retry should not be triggered");
        }
      });
    };

    // test both single and multiGet
    runTest(
        clientConfigBuilder,
        false,
        2,
        fastClientStatsValidation,
        m -> {},
        null,
        Optional.empty(),
        storeMetadataFetchMode);
    runTest(
        clientConfigBuilder,
        true,
        2,
        fastClientStatsValidation,
        m -> {},
        null,
        Optional.empty(),
        storeMetadataFetchMode);
  }

  @Test(dataProvider = "FastClient-One-Boolean", timeOut = TIME_OUT)
  public void testFastClientWithLongTailRetry(boolean batchGet) throws Exception {
    ClientConfig.ClientConfigBuilder clientConfigBuilder =
        new ClientConfig.ClientConfigBuilder<>().setStoreName(storeName).setR2Client(r2Client);

    if (batchGet) {
      clientConfigBuilder
          // default maxAllowedKeyCntInBatchGetReq is 2. configuring it to test different cases.
          .setMaxAllowedKeyCntInBatchGetReq(recordCnt)
          .setUseStreamingBatchGetAsDefault(true);
    }

    Consumer<MetricsRepository> fastClientStatsValidation;
    String metricPrefix;
    String log;
    if (batchGet) {
      metricPrefix = "--multiget_";
      log = "batch Get";
      clientConfigBuilder.setLongTailRetryEnabledForBatchGet(true)
          .setLongTailRetryThresholdForBatchGetInMicroSeconds(1);
    } else {
      metricPrefix = "--";
      log = "single Get";
      clientConfigBuilder.setLongTailRetryEnabledForSingleGet(true)
          .setLongTailRetryThresholdForSingleGetInMicroSeconds(1);
    }
    fastClientStatsValidation = metricsRepository -> {
      assertTrue(
          metricsRepository.metrics()
              .get("." + storeName + metricPrefix + "long_tail_retry_request.OccurrenceRate")
              .value() > 0,
          "Long tail retry for " + log + " should be triggered");
    };
    runTest(
        clientConfigBuilder,
        batchGet,
        recordCnt,
        fastClientStatsValidation,
        m -> {},
        null,
        Optional.empty(),
        StoreMetadataFetchMode.SERVER_BASED_METADATA);
  }
}
