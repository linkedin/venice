package com.linkedin.venice.client.store;

import static com.linkedin.venice.VeniceConstants.COMPUTE_REQUEST_VERSION_V2;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.client.store.streaming.TrackingStreamingCallback;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.client.store.transport.TransportClientStreamingCallback;
import com.linkedin.venice.compute.ComputeRequestWrapper;
import com.linkedin.venice.serializer.RecordSerializer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.avro.Schema;


/**
 * This class should be used for experiments only. It is able to send out requests, but responses from
 * Venice Router will be ignored.
 *
 * TODO: Currently it only works for compute streaming. Need to support single-get, batch-get and regular read compute.
 */
public class AvroBlackHoleResponseStoreClientImpl<K, V> extends AvroGenericStoreClientImpl<K, V> {
  public AvroBlackHoleResponseStoreClientImpl(TransportClient transportClient, ClientConfig clientConfig) {
    super(transportClient, clientConfig);
  }

  @Override
  public void compute(
      ComputeRequestWrapper computeRequestWrapper,
      Set<K> keys,
      Schema resultSchema,
      StreamingCallback<K, ComputeGenericRecord> callback,
      long preRequestTimeInNS) throws VeniceClientException {
    if (handleCallbackForEmptyKeySet(keys, callback)) {
      // empty key set
      return;
    }

    byte[] serializedComputeRequest = serializeComputeRequest(computeRequestWrapper, keys);

    Map<String, String> headerMap = (computeRequestWrapper.getComputeRequestVersion() == COMPUTE_REQUEST_VERSION_V2)
        ? COMPUTE_HEADER_MAP_FOR_STREAMING_V2
        : COMPUTE_HEADER_MAP_FOR_STREAMING_V3;

    getTransportClient().streamPost(
        getComputeRequestPath(),
        headerMap,
        serializedComputeRequest,
        new BlackHoleStreamingCallback<>(keys.size(), callback),
        keys.size());
  }

  private byte[] serializeComputeRequest(ComputeRequestWrapper computeRequestWrapper, Collection<K> keys) {
    RecordSerializer keySerializer = getKeySerializerWithoutRetry();
    List<ByteBuffer> serializedKeyList = new ArrayList<>(keys.size());
    ByteBuffer serializedComputeRequest = ByteBuffer.wrap(computeRequestWrapper.serialize());
    for (K key: keys) {
      serializedKeyList.add(ByteBuffer.wrap(keySerializer.serialize(key)));
    }
    return computeRequestClientKeySerializer.serializeObjects(serializedKeyList, serializedComputeRequest);
  }

  /**
   * BlackHole streaming callback for batch-get/compute.
   *
   * All data chunk returned from Venice Routers will be dropped directly without any deserialization work; when all
   * the chunks have been returned, invoke callbacks so that metrics are reported.
   */
  private class BlackHoleStreamingCallback<ENVELOPE, K, V> implements TransportClientStreamingCallback {
    private final int keySize;
    private final StreamingCallback<K, V> callback;

    private Optional<TrackingStreamingCallback> trackingStreamingCallback = Optional.empty();

    public BlackHoleStreamingCallback(int keySize, StreamingCallback<K, V> callback) {
      this.keySize = keySize;
      this.callback = callback;
      if (callback instanceof TrackingStreamingCallback) {
        trackingStreamingCallback = Optional.of((TrackingStreamingCallback) callback);
      }
    }

    @Override
    public void onHeaderReceived(Map<String, String> headers) {
      // no-op
    }

    @Override
    public void onDataReceived(ByteBuffer chunk) {
      // no-op
    }

    @Override
    public void onCompletion(Optional<VeniceClientException> exception) {
      Optional<Exception> completedException = Optional.empty();
      if (exception.isPresent()) {
        completedException = Optional.of(exception.get());
      }
      callback.onCompletion(completedException);
      final Optional<Exception> finalCompletedException = completedException;
      trackingStreamingCallback.ifPresent(t -> t.onDeserializationCompletion(finalCompletedException, keySize, 0));
    }
  }
}
