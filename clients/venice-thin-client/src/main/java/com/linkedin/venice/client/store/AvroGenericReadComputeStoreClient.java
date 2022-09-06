package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.compute.ComputeRequestWrapper;
import java.io.ByteArrayOutputStream;
import java.util.Optional;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;


/**
 * Venice avro generic client to provide read compute operations
 * @param <K>
 * @param <V>
 */
public interface AvroGenericReadComputeStoreClient<K, V> extends AvroGenericStoreClient<K, V> {
  ComputeRequestBuilder<K> compute(
      final Optional<ClientStats> stats,
      final Optional<ClientStats> streamingStats,
      final long preRequestTimeInNS) throws VeniceClientException;

  void compute(
      ComputeRequestWrapper computeRequestWrapper,
      Set<K> keys,
      Schema resultSchema,
      StreamingCallback<K, GenericRecord> callback,
      final long preRequestTimeInNS) throws VeniceClientException;

  void compute(
      ComputeRequestWrapper computeRequestWrapper,
      Set<K> keys,
      Schema resultSchema,
      StreamingCallback<K, GenericRecord> callback,
      final long preRequestTimeInNS,
      BinaryEncoder reusedEncoder,
      ByteArrayOutputStream reusedOutputStream) throws VeniceClientException;

  void computeWithKeyPrefixFilter(
      byte[] prefixBytes,
      ComputeRequestWrapper computeRequestWrapper,
      StreamingCallback<GenericRecord, GenericRecord> callback);
}
