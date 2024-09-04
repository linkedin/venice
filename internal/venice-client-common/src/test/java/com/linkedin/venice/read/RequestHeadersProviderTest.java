package com.linkedin.venice.read;

import static com.linkedin.venice.HttpConstants.VENICE_API_VERSION;
import static com.linkedin.venice.HttpConstants.VENICE_CLIENT_COMPUTE;
import static com.linkedin.venice.HttpConstants.VENICE_COMPUTE_VALUE_SCHEMA_ID;
import static com.linkedin.venice.HttpConstants.VENICE_KEY_COUNT;
import static com.linkedin.venice.HttpConstants.VENICE_STREAMING;
import static com.linkedin.venice.HttpConstants.VENICE_SUPPORTED_COMPRESSION_STRATEGY;

import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RequestHeadersProviderTest {
  @Test
  public void testThinClientSingleGetHeaders() {
    Map<String, String> headers = RequestHeadersProvider.getThinClientGetHeaderMap();
    Assert.assertEquals(headers.size(), 2);
    Assert.assertTrue(headers.containsKey(VENICE_API_VERSION));
    Assert.assertTrue(headers.containsKey(VENICE_SUPPORTED_COMPRESSION_STRATEGY));
  }

  @Test
  public void testThinClientStreamingBatchGetHeaders() {
    int keyCount = 100;
    Map<String, String> headers = RequestHeadersProvider.getThinClientStreamingBatchGetHeaders(keyCount);
    Assert.assertEquals(headers.size(), 4);
    Assert.assertTrue(headers.containsKey(VENICE_STREAMING));
    Assert.assertTrue(headers.containsKey(VENICE_SUPPORTED_COMPRESSION_STRATEGY));
    Assert.assertTrue(headers.containsKey(VENICE_API_VERSION));
    Assert.assertEquals(headers.get(VENICE_KEY_COUNT), Integer.toString(keyCount));
  }

  @Test
  public void testStreamingBatchGetHeaders() {
    int keyCount = 100;
    Map<String, String> headers = RequestHeadersProvider.getStreamingBatchGetHeaders(keyCount);
    Assert.assertEquals(headers.size(), 2);
    Assert.assertTrue(headers.containsKey(VENICE_API_VERSION));
    Assert.assertEquals(headers.get(VENICE_KEY_COUNT), Integer.toString(keyCount));
  }

  @Test
  public void testStreamingComputeHeaders() {
    int keyCount = 100;
    int computeSchemaId = 3;
    Map<String, String> headers = RequestHeadersProvider.getStreamingComputeHeaderMap(keyCount, computeSchemaId, false);
    Assert.assertEquals(headers.size(), 5);
    Assert.assertTrue(headers.containsKey(VENICE_API_VERSION));
    Assert.assertTrue(headers.containsKey(VENICE_STREAMING));
    Assert.assertTrue(headers.containsKey(VENICE_CLIENT_COMPUTE));
    Assert.assertEquals(headers.get(VENICE_KEY_COUNT), Integer.toString(keyCount));
    Assert.assertEquals(headers.get(VENICE_COMPUTE_VALUE_SCHEMA_ID), Integer.toString(computeSchemaId));

    headers = RequestHeadersProvider.getStreamingComputeHeaderMap(keyCount, computeSchemaId, true);
    Assert.assertEquals(headers.size(), 4);
    Assert.assertFalse(headers.containsKey(VENICE_CLIENT_COMPUTE));
  }
}
