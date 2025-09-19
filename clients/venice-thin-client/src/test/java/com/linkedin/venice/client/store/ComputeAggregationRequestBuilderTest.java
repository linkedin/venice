package com.linkedin.venice.client.store;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertNotNull;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.predicate.LongPredicate;
import com.linkedin.venice.client.store.predicate.Predicate;
import com.linkedin.venice.utils.Time;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;


public class ComputeAggregationRequestBuilderTest {
  static class NoOpComputeAggregationRequestBuilder implements ComputeAggregationRequestBuilder<GenericRecord> {
    @Override
    public ComputeAggregationRequestBuilder countGroupByValue(int topK, String... fieldNames) {
      return this;
    }

    @Override
    public <T> ComputeAggregationRequestBuilder countGroupByBucket(
        Map<String, Predicate<T>> bucketNameToPredicate,
        String... fieldNames) {
      return this;
    }

    @Override
    public CompletableFuture<ComputeAggregationResponse> execute(Set<GenericRecord> keys) throws VeniceClientException {
      return CompletableFuture.completedFuture(mock(ComputeAggregationResponse.class));
    }
  }

  /**
   * This is just an example of how {@link ComputeAggregationRequestBuilder} is expected to be used, but does not actually test
   * anything.
   */
  @Test
  public void exampleUsage() {
    ComputeAggregationRequestBuilder requestBuilder = new NoOpComputeAggregationRequestBuilder();

    long currentTime = System.currentTimeMillis();
    Map<String, Predicate<Long>> bucketByTimeRanges = new HashMap<>();
    bucketByTimeRanges.put("last1Day", LongPredicate.greaterThan(currentTime - 1 * Time.MS_PER_DAY));
    bucketByTimeRanges.put("last7Days", LongPredicate.greaterThan(currentTime - 7 * Time.MS_PER_DAY));
    bucketByTimeRanges.put("last30Days", LongPredicate.greaterThan(currentTime - 30 * Time.MS_PER_DAY));

    Set<GenericRecord> keys = new HashSet<>();
    GenericRecord key1 = mock(GenericRecord.class);
    GenericRecord key2 = mock(GenericRecord.class);
    GenericRecord key3 = mock(GenericRecord.class);
    keys.add(key1);
    keys.add(key2);
    keys.add(key3);

    CompletableFuture<ComputeAggregationResponse> facetCountResponse =
        requestBuilder.countGroupByValue(10, "fieldName1", "fieldName2", "fieldName3")
            .countGroupByValue(20, "fieldName4", "fieldName5", "fieldName6")
            .countGroupByBucket(bucketByTimeRanges, "timeFieldNameA", "timeFieldNameB", "timeFieldNameC")
            .execute(keys);

    assertNotNull(facetCountResponse);
  }
}
