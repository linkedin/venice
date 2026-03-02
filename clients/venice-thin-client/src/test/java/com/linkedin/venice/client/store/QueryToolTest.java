package com.linkedin.venice.client.store;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import com.linkedin.venice.exceptions.VeniceException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.avro.Schema;
import org.mockito.MockedStatic;
import org.testng.Assert;
import org.testng.annotations.Test;


public class QueryToolTest {
  @SuppressWarnings("unchecked")
  private StatTrackingStoreClient<Object, Object> createMockClient(Schema keySchema, Map<Object, Object> batchResults) {
    AbstractAvroStoreClient<Object, Object> mockInnerClient = mock(AbstractAvroStoreClient.class);
    doReturn(keySchema).when(mockInnerClient).getKeySchema();

    StatTrackingStoreClient<Object, Object> mockStatClient = mock(StatTrackingStoreClient.class);
    doReturn(mockInnerClient).when(mockStatClient).getInnerStoreClient();

    CompletableFuture<Map<Object, Object>> future = CompletableFuture.completedFuture(batchResults);
    doReturn(future).when(mockStatClient).batchGet(any());

    return mockStatClient;
  }

  @Test
  public void testBatchQueryStoreForKeys() throws Exception {
    Map<Object, Object> batchResults = new LinkedHashMap<>();
    batchResults.put("key1", "value1");
    // key2 intentionally missing from results to test null value path

    StatTrackingStoreClient<Object, Object> mockClient =
        createMockClient(Schema.create(Schema.Type.STRING), batchResults);

    try (MockedStatic<ClientFactory> mockedFactory = mockStatic(ClientFactory.class)) {
      mockedFactory.when(() -> ClientFactory.getAndStartGenericAvroClient(any(ClientConfig.class)))
          .thenReturn(mockClient);

      Set<String> keys = new LinkedHashSet<>(Arrays.asList("key1", "key2"));
      Map<String, Map<String, String>> results =
          QueryTool.batchQueryStoreForKeys("testStore", keys, "http://localhost:1234", false, Optional.empty());

      Assert.assertEquals(results.size(), 2);
      Assert.assertEquals(results.get("key1").get("key"), "key1");
      Assert.assertEquals(results.get("key1").get("value"), "value1");
      Assert.assertEquals(results.get("key2").get("key"), "key2");
      Assert.assertEquals(results.get("key2").get("value"), "null");
    }
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*SSL configuration is not valid.*")
  public void testBatchQueryStoreForKeysHttpsWithoutSsl() throws Exception {
    Set<String> keys = new LinkedHashSet<>(Arrays.asList("key1"));
    QueryTool.batchQueryStoreForKeys("testStore", keys, "https://localhost:1234", false, Optional.empty());
  }

  @Test
  public void testMainBatchMode() throws Exception {
    Map<Object, Object> batchResults = new LinkedHashMap<>();
    batchResults.put("key1", "value1");

    StatTrackingStoreClient<Object, Object> mockClient =
        createMockClient(Schema.create(Schema.Type.STRING), batchResults);

    try (MockedStatic<ClientFactory> mockedFactory = mockStatic(ClientFactory.class)) {
      mockedFactory.when(() -> ClientFactory.getAndStartGenericAvroClient(any(ClientConfig.class)))
          .thenReturn(mockClient);

      String[] args = { "--batch", "testStore", "http://localhost:1234", "false", "", "key1" };
      QueryTool.main(args);
    }
  }
}
