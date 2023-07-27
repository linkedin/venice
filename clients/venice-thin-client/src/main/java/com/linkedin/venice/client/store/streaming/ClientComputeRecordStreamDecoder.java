package com.linkedin.venice.client.store.streaming;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.read.protocol.response.streaming.StreamingFooterRecordV1;
import com.linkedin.venice.serializer.RecordDeserializer;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;


public class ClientComputeRecordStreamDecoder<K, V> implements RecordStreamDecoder {
  public abstract static class Callback<K, V> extends DelegatingTrackingCallback<K, V> {
    public Callback(TrackingStreamingCallback<K, V> inner) {
      super(inner);
    }

    /** Called for each unprocessed value record found in the stream */
    public abstract void onRawRecordReceived(K key, V value);

    public abstract void onRemoteComputeStateChange(boolean enabled);
  }

  private final Function<Map<String, String>, RecordStreamDecoder> decoderProvider;
  private RecordStreamDecoder decoder = null;

  public ClientComputeRecordStreamDecoder(
      List<K> keyList,
      Callback<K, V> callback,
      Executor deserializationExecutor,
      RecordDeserializer<StreamingFooterRecordV1> streamingFooterRecordDeserializer,
      Supplier<RecordDeserializer<V>> computeDeserializerProvider,
      Function<Integer, RecordDeserializer<V>> valueDeserializerProvider,
      BiFunction<CompressionStrategy, ByteBuffer, ByteBuffer> decompressor) {
    this.decoderProvider = responseHeaders -> {
      if (responseHeaders.containsKey(HttpConstants.VENICE_CLIENT_COMPUTE)) {
        /*
          The assumption is that VENICE_CLIENT_COMPUTE is always present in MultiGet response, and never in Compute response.
          The value of the header indicates a recommended mode for the next Compute request to be sent.
        */
        callback.onRemoteComputeStateChange("0".equals(responseHeaders.get(HttpConstants.VENICE_CLIENT_COMPUTE)));

        TrackingStreamingCallback<K, V> multiGetCallback = new DelegatingTrackingCallback<K, V>(callback) {
          @Override
          public void onRecordReceived(K key, V value) {
            callback.onRawRecordReceived(key, value);
          }
        };
        return new MultiGetRecordStreamDecoder<>(
            keyList,
            multiGetCallback,
            deserializationExecutor,
            streamingFooterRecordDeserializer,
            valueDeserializerProvider,
            decompressor);
      } else {
        return new ComputeRecordStreamDecoder<>(
            keyList,
            callback,
            deserializationExecutor,
            streamingFooterRecordDeserializer,
            computeDeserializerProvider.get());
      }
    };
  }

  @Override
  public void onHeaderReceived(Map<String, String> headers) {
    decoder = decoderProvider.apply(headers);
    decoder.onHeaderReceived(headers);
  }

  @Override
  public void onDataReceived(ByteBuffer chunk) {
    decoder.onDataReceived(chunk);
  }

  @Override
  public void onCompletion(Optional<VeniceClientException> exception) {
    decoder.onCompletion(exception);
  }
}
