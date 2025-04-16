package com.linkedin.venice.fastclient;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.client.store.AvroGenericReadComputeStoreClient;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ComputeGenericRecord;
import com.linkedin.venice.client.store.ComputeRequestBuilder;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.compute.ComputeRequestWrapper;
import com.linkedin.venice.fastclient.factory.ClientFactory;
import com.linkedin.venice.schema.SchemaReader;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.avro.Schema;


/**
 * Inside Fast-Client, we choose to use n-tier architecture style to build a pipeline to separate different
 * types of logic in different layer.
 *
 * <br><br>
 * n-tier architecture => Having multiple layers where each layer wraps the next inner one.
 * Each layer provides some functionality, e.g. stats collection, etc.
 *
 * <br><br>
 * Fast-Client's layers include the below components. Check {@link ClientFactory#getAndStartGenericStoreClient}
 * to figure out how the layers are put together for different requirements.
 *
 * <br><br>
 * Layer -1:<br>
 * {@link AvroGenericStoreClient}, {@link AvroGenericReadComputeStoreClient} => interfaces: Borrowed from thin-client
 *
 * <br><br>
 * Layer 0:<br>
 * {@link InternalAvroStoreClient} implements {@link AvroGenericReadComputeStoreClient} => The abstract class
 * implementing above interfaces for fast-client. All other internal implementations of different tiers should extend
 * this class.
 *
 * <br><br>
 * Layer 1:<br>
 * {@link DispatchingAvroGenericStoreClient} extends {@link InternalAvroStoreClient} => in charge of routing and
 * serialization/de-serialization
 *
 * <br><br>
 * Layer 2:<br>
 * {@link RetriableAvroGenericStoreClient} extends {@link DelegatingAvroStoreClient} => Adds optional retry ability on
 * top of DispatchingAvroGenericStoreClient
 *
 * <br><br>
 * Layer 3:<br>
 * {@link StatsAvroGenericStoreClient} extends {@link DelegatingAvroStoreClient} => Adds stats on top of Layer 2 or
 * Layer 1. There is no option to disable it, but if needed, can be disabled.
 *
 * <br><br>
 * Layer 4:<br>
 * {@link DualReadAvroGenericStoreClient} extends {@link DelegatingAvroStoreClient} => Adds an extra read via thin
 * client on top of Layer 3.
 *
 * <br><br>
 * utils class:<br>
 * {@link DelegatingAvroStoreClient} extends {@link InternalAvroStoreClient} => Delegator pattern to not override all
 * the functions in every superclass in a duplicate manner.
 *
 * <br><br>
 * Interactions between these class for some flows: https://swimlanes.io/u/iHTCBvlf0
 */

public class DelegatingAvroStoreClient<K, V> extends InternalAvroStoreClient<K, V> {
  private final InternalAvroStoreClient<K, V> delegate;
  private final ClientConfig clientConfig;

  public DelegatingAvroStoreClient(InternalAvroStoreClient<K, V> delegate, ClientConfig clientConfig) {
    this.delegate = delegate;
    this.clientConfig = clientConfig;
  }

  @Override
  public ClientConfig getClientConfig() {
    return clientConfig;
  }

  @Override
  public SchemaReader getSchemaReader() {
    return delegate.getSchemaReader();
  }

  @Override
  protected CompletableFuture<V> get(GetRequestContext<K> requestContext, K key) throws VeniceClientException {
    return delegate.get(requestContext, key);
  }

  @Override
  protected void streamingBatchGet(
      BatchGetRequestContext<K, V> requestContext,
      Set<K> keys,
      StreamingCallback<K, V> callback) {
    delegate.streamingBatchGet(requestContext, keys, callback);
  }

  @Override
  protected void compute(
      ComputeRequestContext<K, V> requestContext,
      ComputeRequestWrapper computeRequestWrapper,
      Set<K> keys,
      Schema resultSchema,
      StreamingCallback<K, ComputeGenericRecord> callback,
      long preRequestTimeInNS) throws VeniceClientException {
    delegate.compute(requestContext, computeRequestWrapper, keys, resultSchema, callback, preRequestTimeInNS);
  }

  @Override
  public void start() throws VeniceClientException {
    delegate.start();
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public String getStoreName() {
    return delegate.getStoreName();
  }

  @Override
  public Schema getKeySchema() {
    return delegate.getKeySchema();
  }

  @Deprecated
  @Override
  public Schema getLatestValueSchema() {
    return delegate.getLatestValueSchema();
  }

  @Override
  public ComputeRequestBuilder<K> compute(
      Optional<ClientStats> stats,
      Optional<ClientStats> streamingStats,
      AvroGenericReadComputeStoreClient computeStoreClient,
      long preRequestTimeInNS) throws VeniceClientException {
    return delegate.compute(stats, streamingStats, computeStoreClient, preRequestTimeInNS);
  }
}
