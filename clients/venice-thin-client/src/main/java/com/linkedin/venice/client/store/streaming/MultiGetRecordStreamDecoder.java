package com.linkedin.venice.client.store.streaming;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.read.protocol.response.MultiGetResponseRecordV1;
import com.linkedin.venice.read.protocol.response.streaming.StreamingFooterRecordV1;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Function;


public class MultiGetRecordStreamDecoder<K, V> extends AbstractRecordStreamDecoder<MultiGetResponseRecordV1, K, V> {
  private final Map<Integer, RecordDeserializer<V>> deserializerCache = new VeniceConcurrentHashMap<>();
  private final RecordDeserializer<StreamingFooterRecordV1> streamingFooterDeserializer;
  private final Function<Integer, RecordDeserializer<V>> valueDeserializerProvider;
  private final BiFunction<CompressionStrategy, ByteBuffer, ByteBuffer> decompressor;

  public MultiGetRecordStreamDecoder(
      List<K> keyList,
      TrackingStreamingCallback<K, V> callback,
      Executor deserializationExecutor,
      RecordDeserializer<StreamingFooterRecordV1> streamingFooterDeserializer,
      Function<Integer, RecordDeserializer<V>> valueDeserializerProvider,
      BiFunction<CompressionStrategy, ByteBuffer, ByteBuffer> decompressor) {
    super(keyList, callback, deserializationExecutor);
    this.streamingFooterDeserializer = streamingFooterDeserializer;
    this.valueDeserializerProvider = valueDeserializerProvider;
    this.decompressor = decompressor;
  }

  @Override
  protected ReadEnvelopeChunkedDeserializer<MultiGetResponseRecordV1> getEnvelopeDeserializer(int schemaId) {
    int protocolVersion = ReadAvroProtocolDefinition.MULTI_GET_RESPONSE_V1.getProtocolVersion();
    if (protocolVersion != schemaId) {
      throw new VeniceClientException("schemaId: " + schemaId + " is not expected, should be " + protocolVersion);
    }
    return new MultiGetResponseRecordV1ChunkedDeserializer();
  }

  @Override
  protected StreamingFooterRecordV1 getStreamingFooterRecord(MultiGetResponseRecordV1 envelope) {
    return streamingFooterDeserializer.deserialize(envelope.value);
  }

  @Override
  protected V getValueRecord(MultiGetResponseRecordV1 envelope, CompressionStrategy compression) {
    if (!envelope.value.hasRemaining()) {
      // Safeguard to handle empty value, which indicates non-existing key.
      return null;
    }
    RecordDeserializer<V> deserializer =
        deserializerCache.computeIfAbsent(envelope.schemaId, valueDeserializerProvider);
    ByteBuffer decompressedValue = decompressor.apply(compression, envelope.value);
    return deserializer.deserialize(decompressedValue);
  }

  @Override
  protected int getKeyIndex(MultiGetResponseRecordV1 envelope) {
    return envelope.getKeyIndex();
  }
}
