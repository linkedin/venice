package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.client.store.streaming.VeniceResponseMap;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.avro.generic.GenericRecord;


public interface ComputeRequestBuilder<K> {

  /**
   * Setup project fields, and right now only top-level fields are supported.
   * @param fieldNames
   * @return
   */
  ComputeRequestBuilder<K> project(String ... fieldNames);

  /**
   * Setup project fields, and right now only top-level fields are supported.
   * @param fieldNames
   * @return
   */
  ComputeRequestBuilder<K> project(Collection<String> fieldNames);

  /**
   * Setup dot-product operation.
   * @param inputFieldName : top-level field in the value record as the input of dot-product operation
   * @param dotProductParam : dot-product param
   * @param resultFieldName : result field name in the response record
   * @return
   */
  ComputeRequestBuilder<K> dotProduct(String inputFieldName, List<Float> dotProductParam, String resultFieldName);

  /**
   * Setup cosine-similarity operation.
   * @param inputFieldName : top-level field in the value record as the input of cosine-similarity operation
   * @param cosSimilarityParam : cosine-similarity param
   * @param resultFieldName : result field name in the response record
   * @return
   */
  ComputeRequestBuilder<K> cosineSimilarity(String inputFieldName, List<Float> cosSimilarityParam, String resultFieldName);

  /**
   * Setup hadamard-product operation; if this api is invoked, use version 2 in the compute request version header.
   * @param inputFieldName : top-level field in the value record as the input of hadamard-product operation
   * @param hadamardProductParam : hadamard-product param
   * @param resultFieldName : result field name in the response record
   * @return
   */
  ComputeRequestBuilder<K> hadamardProduct(String inputFieldName, List<Float> hadamardProductParam, String resultFieldName);

  /**
   * Send compute request to Venice, and this should be the last step of the compute specification.
   * @param keys : keys for the candidate records
   * @return
   * @throws VeniceClientException
   */
  CompletableFuture<Map<K, GenericRecord>> execute(Set<K> keys) throws VeniceClientException;

  /**
   * Send compute request to Venice, and this should be the last step of the compute specification.
   * The difference between this function and the previous {@link #execute(Set)} is that this function will return
   * the available response instead of throwing a {@link java.util.concurrent.TimeoutException} when timeout happens:
   * streamingExecute(keys).get(timeout, units);
   *
   * @param keys
   * @return
   * @throws VeniceClientException
   */
  CompletableFuture<VeniceResponseMap<K, GenericRecord>> streamingExecute(Set<K> keys) throws VeniceClientException;

  /**
   * Streaming interface for {@link #execute(Set)}, and you could find more info in {@link StreamingCallback}.
   * @param keys
   * @param callback
   * @throws VeniceClientException
   */
  void streamingExecute(Set<K> keys, StreamingCallback<K, GenericRecord> callback) throws VeniceClientException;

}