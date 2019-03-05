package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.avro.generic.GenericRecord;


public interface ComputeRequestBuilder<K> {

  ComputeRequestBuilder<K> project(String ... fieldNames);

  ComputeRequestBuilder<K> project(Collection<String> fieldNames);

  ComputeRequestBuilder<K> dotProduct(String inputFieldName, List<Float> dotProductParam, String resultFieldName);

  ComputeRequestBuilder<K> cosineSimilarity(String inputFieldName, List<Float> cosSimilarityParam, String resultFieldName);

  CompletableFuture<Map<K, GenericRecord>> execute(Set<K> keys) throws VeniceClientException;

}