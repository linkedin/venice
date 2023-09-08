package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.compute.ComputeRequestWrapper;
import java.util.Optional;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


/**
 * Venice avro generic client to provide read compute operations
 * @param <K>
 * @param <V>
 */
public interface AvroGenericReadComputeStoreClient<K, V> extends AvroGenericStoreClient<K, V> {
  boolean isProjectionFieldValidationEnabled();

  @Override
  default ComputeRequestBuilder<K> compute() throws VeniceClientException {
    return compute(Optional.empty(), Optional.empty(), 0);
  }

  default ComputeRequestBuilder<K> compute(
      Optional<ClientStats> stats,
      Optional<ClientStats> streamingStats,
      long preRequestTimeInNS) throws VeniceClientException {
    return compute(stats, streamingStats, this, preRequestTimeInNS);
  }

  default ComputeRequestBuilder<K> compute(
      Optional<ClientStats> stats,
      Optional<ClientStats> streamingStats,
      AvroGenericReadComputeStoreClient computeStoreClient,
      long preRequestTimeInNS) throws VeniceClientException {
    return new AvroComputeRequestBuilderV3<K>(computeStoreClient, getLatestValueSchema()).setStats(streamingStats)
        .setValidateProjectionFields(isProjectionFieldValidationEnabled());
  }

  void compute(
      ComputeRequestWrapper computeRequestWrapper,
      Set<K> keys,
      Schema resultSchema,
      StreamingCallback<K, ComputeGenericRecord> callback,
      long preRequestTimeInNS) throws VeniceClientException;

  void computeWithKeyPrefixFilter(
      byte[] keyPrefix,
      ComputeRequestWrapper computeRequestWrapper,
      StreamingCallback<GenericRecord, GenericRecord> callback) throws VeniceClientException;
}
