package com.linkedin.venice.featurematrix.validation;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.featurematrix.model.FeatureDimensions.ClientType;
import com.linkedin.venice.featurematrix.model.TestCaseConfig;
import com.linkedin.venice.featurematrix.setup.ClientFactory.ReadClientWrapper;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;


/**
 * Validates data integrity for single get and batch get operations.
 * Checks that all written records can be read back correctly through the configured client.
 */
public class DataIntegrityValidator {
  private static final Logger LOGGER = LogManager.getLogger(DataIntegrityValidator.class);

  /**
   * Validates single get for all expected key-value pairs.
   */
  public static void validateSingleGet(
      ReadClientWrapper clientWrapper,
      Map<String, String> expectedData,
      TestCaseConfig config) throws ExecutionException, InterruptedException {
    LOGGER
        .info("Validating single get for {} keys using {} client", expectedData.size(), clientWrapper.getClientType());

    AvroGenericStoreClient<Object, Object> client = clientWrapper.getStoreClient();
    if (client == null && clientWrapper.getClientType() == ClientType.DA_VINCI) {
      LOGGER.info("DaVinci client validation delegated to DaVinci-specific validator");
      return;
    }

    for (Map.Entry<String, String> entry: expectedData.entrySet()) {
      Object value = client.get(entry.getKey()).get();
      Assert.assertNotNull(
          value,
          String.format(
              "Single get returned null for key '%s' (TC%d, client=%s)",
              entry.getKey(),
              config.getTestCaseId(),
              clientWrapper.getClientType()));
      Assert.assertEquals(
          value.toString(),
          entry.getValue(),
          String.format(
              "Single get returned wrong value for key '%s' (TC%d, client=%s)",
              entry.getKey(),
              config.getTestCaseId(),
              clientWrapper.getClientType()));
    }

    LOGGER.info("Single get validation passed for {} keys", expectedData.size());
  }

  /**
   * Validates batch get for all expected key-value pairs.
   */
  public static void validateBatchGet(
      ReadClientWrapper clientWrapper,
      Map<String, String> expectedData,
      TestCaseConfig config) throws ExecutionException, InterruptedException {
    LOGGER.info("Validating batch get for {} keys using {} client", expectedData.size(), clientWrapper.getClientType());

    AvroGenericStoreClient<Object, Object> client = clientWrapper.getStoreClient();
    if (client == null) {
      LOGGER.info("Skipping batch get validation - no store client available");
      return;
    }

    Set<Object> keys = new HashSet<>(expectedData.keySet());
    Map<Object, Object> results = client.batchGet(keys).get();

    Assert.assertEquals(
        results.size(),
        expectedData.size(),
        String.format(
            "Batch get returned %d results but expected %d (TC%d, client=%s)",
            results.size(),
            expectedData.size(),
            config.getTestCaseId(),
            clientWrapper.getClientType()));

    for (Map.Entry<String, String> entry: expectedData.entrySet()) {
      Object value = results.get(entry.getKey());
      Assert.assertNotNull(
          value,
          String.format(
              "Batch get missing key '%s' (TC%d, client=%s)",
              entry.getKey(),
              config.getTestCaseId(),
              clientWrapper.getClientType()));
      Assert.assertEquals(
          value.toString(),
          entry.getValue(),
          String.format(
              "Batch get wrong value for key '%s' (TC%d, client=%s)",
              entry.getKey(),
              config.getTestCaseId(),
              clientWrapper.getClientType()));
    }

    LOGGER.info("Batch get validation passed for {} keys", expectedData.size());
  }

  /**
   * Validates that chunked records (W5=on) are correctly reassembled.
   * Writes a large value that exceeds the chunk size threshold and verifies it reads back correctly.
   */
  public static void validateChunking(
      ReadClientWrapper clientWrapper,
      String largeKey,
      String largeValue,
      TestCaseConfig config) throws ExecutionException, InterruptedException {
    if (!config.isChunking()) {
      return;
    }

    LOGGER.info("Validating chunking for key '{}' (value size: {} bytes)", largeKey, largeValue.length());

    AvroGenericStoreClient<Object, Object> client = clientWrapper.getStoreClient();
    if (client == null) {
      return;
    }

    Object value = client.get(largeKey).get();
    Assert.assertNotNull(value, "Chunked value returned null for key '" + largeKey + "'");
    Assert.assertEquals(
        value.toString(),
        largeValue,
        "Chunked value mismatch for key '" + largeKey + "' (TC" + config.getTestCaseId() + ")");

    LOGGER.info("Chunking validation passed");
  }
}
