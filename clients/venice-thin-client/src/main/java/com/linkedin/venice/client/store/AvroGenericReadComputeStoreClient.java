package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.compute.ComputeRequestWrapper;
import com.linkedin.venice.schema.SchemaReader;
import java.util.Optional;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;


/**
 * Venice avro generic client to provide read compute operations. This interface is internal and is subject to change.
 * @param <K>
 * @param <V>
 */
public interface AvroGenericReadComputeStoreClient<K, V> extends AvroGenericStoreClient<K, V> {
  SchemaReader getSchemaReader();

  boolean isProjectionFieldValidationEnabled();

  @Override
  default ComputeRequestBuilder<K> compute() throws VeniceClientException {
    return compute(Optional.empty(), this);
  }

  @Deprecated
  default ComputeRequestBuilder<K> compute(
      Optional<ClientStats> stats,
      Optional<ClientStats> streamingStats,
      long preRequestTimeInNS) throws VeniceClientException {
    return compute(streamingStats, this);
  }

  @Deprecated
  default ComputeRequestBuilder<K> compute(
      Optional<ClientStats> stats,
      Optional<ClientStats> streamingStats,
      AvroGenericReadComputeStoreClient computeStoreClient,
      long preRequestTimeInNS) throws VeniceClientException {
    return compute(stats, computeStoreClient);
  }

  default ComputeRequestBuilder<K> compute(
      Optional<ClientStats> stats,
      AvroGenericReadComputeStoreClient computeStoreClient) throws VeniceClientException {
    return new AvroComputeRequestBuilderV3<K>(computeStoreClient, getSchemaReader()).setStats(stats)
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
