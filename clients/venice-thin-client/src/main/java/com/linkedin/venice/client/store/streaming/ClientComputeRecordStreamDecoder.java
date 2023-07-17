package com.linkedin.venice.client.store.streaming;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.transport.TransportClientStreamingCallback;
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


public class ClientComputeRecordStreamDecoder<K, V> implements TransportClientStreamingCallback {
  private final Function<Map<String, String>, TransportClientStreamingCallback> decoderProvider;
  private TransportClientStreamingCallback decoder = null;

  public ClientComputeRecordStreamDecoder(
      List<K> keyList,
      ClientComputeStreamingCallback<K, V> callback,
      StreamingCallback trackingCallback,
      Executor deserializationExecutor,
      RecordDeserializer<StreamingFooterRecordV1> streamingFooterRecordDeserializer,
      Supplier<RecordDeserializer<V>> computeDeserializerProvider,
      Function<Integer, RecordDeserializer<V>> valueDeserializerProvider,
      BiFunction<CompressionStrategy, ByteBuffer, ByteBuffer> decompressor) {
    this.decoderProvider = headers -> {
      if (headers.containsKey(HttpConstants.VENICE_CLIENT_COMPUTE)) {
        callback.onRemoteComputeStateChange("0".equals(headers.get(HttpConstants.VENICE_CLIENT_COMPUTE)));
        return new MultiGetRecordStreamDecoder<>(keyList, new StreamingCallback<K, V>() {
          @Override
          public void onRecordReceived(K key, V value) {
            callback.onRawRecordReceived(key, value);
          }

          @Override
          public void onCompletion(Optional<Exception> exception) {
            callback.onCompletion(exception);
          }
        },
            trackingCallback,
            deserializationExecutor,
            streamingFooterRecordDeserializer,
            valueDeserializerProvider,
            decompressor);
      } else {
        return new ComputeRecordStreamDecoder<>(
            keyList,
            callback,
            trackingCallback,
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
