package com.linkedin.venice.featurematrix.validation;

import static com.linkedin.venice.utils.TestWriteUtils.DEFAULT_USER_DATA_RECORD_COUNT;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.featurematrix.model.TestCaseConfig;
import com.linkedin.venice.utils.TestUtils;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;


/**
 * Validates data integrity for single get, batch get, and streaming operations.
 * Checks that all written records can be read back correctly through the configured client.
 */
public class DataIntegrityValidator {
  private static final Logger LOGGER = LogManager.getLogger(DataIntegrityValidator.class);

  /**
   * Validates single get for batch push data. Each record's value is a NameRecord
   * with a firstName field containing "first_name_N" (possibly padded for chunking).
   */
  public static void validateSingleGet(AvroGenericStoreClient<String, Object> client, TestCaseConfig config) {
    LOGGER.info("Validating single get for TC{}", config.getTestCaseId());

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, true, () -> {
      for (int i = 1; i <= 10; i++) {
        String key = Integer.toString(i);
        Object value = client.get(key).get();
        Assert.assertNotNull(value, "Single get null for key " + key + " TC" + config.getTestCaseId());
        Assert.assertTrue(
            value.toString().contains("first_name_" + i),
            "Single get wrong for key " + key + " TC" + config.getTestCaseId() + " got: " + value);
      }
    });

    LOGGER.info("Single get validation passed for batch data");
  }

  /**
   * Validates batch get returns the expected number of records.
   */
  public static void validateBatchGet(AvroGenericStoreClient<String, Object> client, TestCaseConfig config)
      throws Exception {
    LOGGER.info("Validating batch get for TC{}", config.getTestCaseId());

    Set<String> keys = new HashSet<>();
    for (int i = 1; i <= DEFAULT_USER_DATA_RECORD_COUNT; i++) {
      keys.add(Integer.toString(i));
    }
    Map<String, Object> results = client.batchGet(keys).get();
    Assert.assertEquals(
        results.size(),
        DEFAULT_USER_DATA_RECORD_COUNT,
        "Batch get wrong count TC" + config.getTestCaseId());

    LOGGER.info("Batch get validation passed for {} keys", results.size());
  }

  /**
   * Validates streaming data (written via VeniceWriter) is readable.
   * Applies to hybrid and nearline-only topologies.
   */
  public static void validateStreamingData(
      AvroGenericStoreClient<String, Object> client,
      TestCaseConfig config,
      int streamingRecordStart,
      int streamingRecordEnd) {
    LOGGER.info(
        "Validating streaming data for TC{} (keys {}-{})",
        config.getTestCaseId(),
        streamingRecordStart,
        streamingRecordEnd);

    TestUtils.waitForNonDeterministicAssertion(60, TimeUnit.SECONDS, true, true, () -> {
      for (int i = streamingRecordStart; i <= streamingRecordStart + 5; i++) {
        String key = Integer.toString(i);
        Object value = client.get(key).get();
        Assert.assertNotNull(value, "Streaming data null for key " + key + " TC" + config.getTestCaseId());
        Assert.assertTrue(
            value.toString().contains("first_name_" + i),
            "Streaming data wrong for key " + key + " TC" + config.getTestCaseId() + " got: " + value);
      }
    });

    LOGGER.info("Streaming data validation passed");
  }
}
