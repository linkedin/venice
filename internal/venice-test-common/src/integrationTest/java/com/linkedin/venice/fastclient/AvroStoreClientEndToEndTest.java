package com.linkedin.venice.fastclient;

import static com.linkedin.venice.utils.Time.MS_PER_SECOND;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ComputeGenericRecord;
import com.linkedin.venice.fastclient.meta.StoreMetadataFetchMode;
import com.linkedin.venice.fastclient.schema.TestValueSchema;
import com.linkedin.venice.fastclient.utils.AbstractClientEndToEndSetup;
import com.linkedin.venice.fastclient.utils.ClientTestUtils;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.stats.VeniceMetricsRepository;
import com.linkedin.venice.utils.TestUtils;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
 */

public class AvroStoreClientEndToEndTest extends AbstractClientEndToEndSetup {
  /**
   * Run fast client tests based on the input parameters.
   *
   * @param clientConfigBuilder config to build client
   * @param requestType singleGet or batchGet or compute
   * @throws Exception
   */
  private void runTest(
      ClientConfig.ClientConfigBuilder clientConfigBuilder,
      RequestType requestType,
      Consumer<MetricsRepository> fastClientStatsValidation,
      Consumer<MetricsRepository> thinClientStatsValidation,
      MetricsRepository thinClientMetricsRepository,
      Optional<AvroGenericStoreClient> vsonThinClient,
      StoreMetadataFetchMode storeMetadataFetchMode,
      boolean emitTehutiMetrics) throws Exception {
    VeniceMetricsRepository metricsRepositoryForGenericClient = createVeniceMetricsRepository(emitTehutiMetrics);
    AvroGenericStoreClient<String, GenericRecord> genericFastClient = null;
    AvroGenericStoreClient<String, Object> genericFastVsonClient = null;
    boolean batchGet = requestType == RequestType.MULTI_GET || requestType == RequestType.MULTI_GET_STREAMING;
    boolean compute = requestType == RequestType.COMPUTE || requestType == RequestType.COMPUTE_STREAMING;
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
        // test batch get of size recordCnt (configured)
        Set<String> keys = new HashSet<>();
        for (int i = 0; i < recordCnt; ++i) {
          String key = keyPrefix + i;
          keys.add(key);
        }
        keys.add("nonExistingKey");
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
          assertTrue(vsonResultObj instanceof Map, "VsonClient should return Map, but got" + vsonResultObj.getClass());
          Map vsonResult = (Map) vsonResultObj;
          assertEquals((int) vsonResult.get(VALUE_FIELD_NAME), i);
        }
      } else if (compute) {
        // test compute of size recordCnt (configured)
        Set<String> keys = new HashSet<>();
        for (int i = 0; i < recordCnt; ++i) {
          String key = keyPrefix + i;
          keys.add(key);
        }
        keys.add("nonExistingKey");
        Map<String, ComputeGenericRecord> resultMap =
            genericFastClient.compute().project(VALUE_FIELD_NAME).execute(keys).get();
        assertEquals(resultMap.size(), recordCnt);

        for (int i = 0; i < recordCnt; ++i) {
          String key = keyPrefix + i;
          assertEquals((int) resultMap.get(key).get(VALUE_FIELD_NAME), i);
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
        assertNull(genericFastClient.get("nonExistingKey").get());
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
    VeniceMetricsRepository metricsRepositoryForSpecificClient = createVeniceMetricsRepository(emitTehutiMetrics);
    ClientConfig.ClientConfigBuilder specificClientConfigBuilder = clientConfigBuilder.clone();
    AvroSpecificStoreClient<String, TestValueSchema> specificFastClient = getSpecificFastClient(
        specificClientConfigBuilder,
        metricsRepositoryForSpecificClient,
        TestValueSchema.class,
        storeMetadataFetchMode);
    try {
      if (batchGet) {
        // test batch get of size recordCnt (configured)
        Set<String> keys = new HashSet<>();
        for (int i = 0; i < recordCnt; ++i) {
          String key = keyPrefix + i;
          keys.add(key);
        }
        keys.add("nonExistingKey");
        Map<String, TestValueSchema> resultMap = specificFastClient.batchGet(keys).get();
        assertEquals(resultMap.size(), recordCnt);

        for (int i = 0; i < recordCnt; ++i) {
          String key = keyPrefix + i;
          assertEquals(resultMap.get(key).int_field, i);
        }
      } else if (compute) { // Compute response is same in both generic and specific clients since the result is always
                            // a ComputeGenericRecord
        // test compute of size recordCnt (configured)
        Set<String> keys = new HashSet<>();
        for (int i = 0; i < recordCnt; ++i) {
          String key = keyPrefix + i;
          keys.add(key);
        }
        keys.add("nonExistingKey");
        Map<String, ComputeGenericRecord> resultMap =
            specificFastClient.compute().project(VALUE_FIELD_NAME).execute(keys).get();
        assertEquals(resultMap.size(), recordCnt);

        for (int i = 0; i < recordCnt; ++i) {
          String key = keyPrefix + i;
          assertEquals(resultMap.get(key).get(VALUE_FIELD_NAME), i);
        }
      } else {
        for (int i = 0; i < recordCnt; ++i) {
          String key = keyPrefix + i;
          TestValueSchema value = specificFastClient.get(key).get();
          assertEquals(value.int_field, i);
        }
        assertNull(specificFastClient.get("nonExistingKey").get());
      }
      fastClientStatsValidation.accept(metricsRepositoryForSpecificClient);
    } finally {
      if (specificFastClient != null) {
        specificFastClient.close();
      }
    }
  }

  @Test(dataProvider = "FastClient-Test-Permutations", timeOut = TIME_OUT)
  public void testFastClientGet(
      boolean dualRead,
      boolean enableGrpc,
      boolean retryEnabled,
      RequestType requestType,
      StoreMetadataFetchMode storeMetadataFetchMode,
      boolean emitTehutiMetrics) throws Exception {
    boolean batchGet = requestType == RequestType.MULTI_GET || requestType == RequestType.MULTI_GET_STREAMING;
    boolean compute = requestType == RequestType.COMPUTE || requestType == RequestType.COMPUTE_STREAMING;

    ClientConfig.ClientConfigBuilder clientConfigBuilder =
        new ClientConfig.ClientConfigBuilder<>().setStoreName(storeName)
            .setR2Client(r2Client)
            .setDualReadEnabled(dualRead);
    // Test HAR algorithm in this test.
    Set<String> harClusters = new HashSet<>();
    harClusters.add(veniceCluster.getServerD2ServiceName());
    clientConfigBuilder.setHARClusters(harClusters);

    if (enableGrpc) {
      setUpGrpcFastClient(clientConfigBuilder);
    }

    if (retryEnabled) {
      // Enable retry code paths but avoid actual retries by using large thresholds
      clientConfigBuilder.setLongTailRetryEnabledForSingleGet(true)
          .setLongTailRetryThresholdForSingleGetInMicroSeconds(TIME_OUT * MS_PER_SECOND)
          // Use explicit batch-get thresholds string to avoid dynamic surprises
          .setLongTailRangeBasedRetryThresholdForBatchGetInMilliSeconds("1-:10000")
          .setLongTailRetryEnabledForCompute(true)
          .setLongTailRangeBasedRetryThresholdForComputeInMilliSeconds("1-:10000");
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

      Consumer<MetricsRepository> fastClientStatsValidation;
      if (batchGet) {
        fastClientStatsValidation = (metricsRepository) -> validateBatchGetMetrics(
            metricsRepository,
            false,
            recordCnt + 1,
            recordCnt,
            false,
            emitTehutiMetrics);
      } else if (compute) {
        fastClientStatsValidation = metricsRepository -> validateComputeMetrics(
            metricsRepository,
            false,
            recordCnt + 1,
            recordCnt,
            false,
            emitTehutiMetrics);
      } else {
        fastClientStatsValidation =
            metricsRepository -> validateSingleGetMetrics(metricsRepository, false, emitTehutiMetrics);
      }
      runTest(
          clientConfigBuilder,
          requestType,
          fastClientStatsValidation,
          thinClientStatsValidation,
          thinClientMetricsRepository,
          dualRead ? Optional.of(genericVsonThinClient) : Optional.empty(),
          storeMetadataFetchMode,
          emitTehutiMetrics);
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

  @Test(dataProvider = "fastClientHTTPVariantsAndStoreMetadataFetchModes", timeOut = 2 * TIME_OUT)
  public void testFastClientGetWithDifferentHTTPVariants(
      ClientTestUtils.FastClientHTTPVariant fastClientHTTPVariant,
      StoreMetadataFetchMode storeMetadataFetchMode) throws Exception {
    Client r2Client = ClientTestUtils.getR2Client(fastClientHTTPVariant);
    ClientConfig.ClientConfigBuilder clientConfigBuilder =
        new ClientConfig.ClientConfigBuilder<>().setStoreName(storeName)
            .setR2Client(r2Client)
            .setDualReadEnabled(false);
    // single get
    Consumer<MetricsRepository> fastClientStatsValidation =
        metricsRepository -> validateSingleGetMetrics(metricsRepository, false, false);

    runTest(
        clientConfigBuilder,
        RequestType.SINGLE_GET,
        fastClientStatsValidation,
        m -> {},
        null,
        Optional.empty(),
        storeMetadataFetchMode,
        false);

    // batch get
    fastClientStatsValidation =
        metricsRepository -> validateBatchGetMetrics(metricsRepository, false, recordCnt, recordCnt, false, false);
    runTest(
        clientConfigBuilder,
        RequestType.MULTI_GET,
        fastClientStatsValidation,
        m -> {},
        null,
        Optional.empty(),
        storeMetadataFetchMode,
        false);

    // compute
    fastClientStatsValidation =
        metricsRepository -> validateComputeMetrics(metricsRepository, false, recordCnt, recordCnt, false, false);
    runTest(
        clientConfigBuilder,
        RequestType.COMPUTE,
        fastClientStatsValidation,
        m -> {},
        null,
        Optional.empty(),
        storeMetadataFetchMode,
        false);
  }

  @Test(groups = { "flaky" }, dataProvider = "FastClient-Request-Types-Small", timeOut = TIME_OUT)
  public void testFastClientWithLongTailRetry(RequestType requestType) throws Exception {
    boolean batchGet = requestType == RequestType.MULTI_GET || requestType == RequestType.MULTI_GET_STREAMING;
    boolean compute = requestType == RequestType.COMPUTE || requestType == RequestType.COMPUTE_STREAMING;

    ClientConfig.ClientConfigBuilder clientConfigBuilder =
        new ClientConfig.ClientConfigBuilder<>().setStoreName(storeName).setR2Client(r2Client);

    Consumer<MetricsRepository> fastClientStatsValidation;
    if (batchGet) {
      clientConfigBuilder.setLongTailRetryEnabledForBatchGet(true)
          .setLongTailRetryThresholdForBatchGetInMicroSeconds(1);
      fastClientStatsValidation =
          metricsRepository -> validateBatchGetMetrics(metricsRepository, false, recordCnt, recordCnt, true, true);
    } else if (compute) {
      clientConfigBuilder.setLongTailRetryEnabledForCompute(true)
          .setLongTailRangeBasedRetryThresholdForComputeInMilliSeconds("1-:1");
      fastClientStatsValidation =
          metricsRepository -> validateComputeMetrics(metricsRepository, false, recordCnt, recordCnt, true, true);
    } else {
      clientConfigBuilder.setLongTailRetryEnabledForSingleGet(true)
          .setLongTailRetryThresholdForSingleGetInMicroSeconds(1);
      fastClientStatsValidation = metricsRepository -> validateSingleGetMetrics(metricsRepository, true, true);
    }
    runTest(
        clientConfigBuilder,
        requestType,
        fastClientStatsValidation,
        m -> {},
        null,
        Optional.empty(),
        StoreMetadataFetchMode.SERVER_BASED_METADATA,
        true);
  }
}
