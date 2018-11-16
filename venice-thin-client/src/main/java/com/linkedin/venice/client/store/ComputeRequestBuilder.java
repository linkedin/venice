package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.avro.generic.GenericRecord;


public interface ComputeRequestBuilder<K> {

  ComputeRequestBuilder project(String ... fieldNames);

  ComputeRequestBuilder dotProduct(String inputFieldName, Float[] dotProductParam, String resultFieldName);

  CompletableFuture<Map<K, GenericRecord>> execute(Set<K> keys) throws VeniceClientException;

}
