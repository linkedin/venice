package com.linkedin.venice.client.store.streaming;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.compute.protocol.response.ComputeResponseRecordV1;
import com.linkedin.venice.read.protocol.response.streaming.StreamingFooterRecordV1;
import com.linkedin.venice.schema.avro.ReadAvroProtocolDefinition;
import com.linkedin.venice.serializer.RecordDeserializer;
import java.util.List;
import java.util.concurrent.Executor;


public class ComputeRecordStreamDecoder<K, V> extends AbstractRecordStreamDecoder<ComputeResponseRecordV1, K, V> {
  private final RecordDeserializer<StreamingFooterRecordV1> streamingFooterDeserializer;
  private final RecordDeserializer<V> valueDeserializer;

  public ComputeRecordStreamDecoder(
      List<K> keyList,
      TrackingStreamingCallback<K, V> callback,
      Executor deserializationExecutor,
      RecordDeserializer<StreamingFooterRecordV1> streamingFooterDeserializer,
      RecordDeserializer<V> valueDeserializer) {
    super(keyList, callback, deserializationExecutor);
    this.streamingFooterDeserializer = streamingFooterDeserializer;
    this.valueDeserializer = valueDeserializer;
  }

  @Override
  protected ReadEnvelopeChunkedDeserializer<ComputeResponseRecordV1> getEnvelopeDeserializer(int schemaId) {
    int protocolVersion = ReadAvroProtocolDefinition.MULTI_GET_RESPONSE_V1.getProtocolVersion();
    if (protocolVersion != schemaId) {
      throw new VeniceClientException("schemaId: " + schemaId + " is not expected, should be " + protocolVersion);
    }
    return new ComputeResponseRecordV1ChunkedDeserializer();
  }

  @Override
  protected StreamingFooterRecordV1 getStreamingFooterRecord(ComputeResponseRecordV1 envelope) {
    return streamingFooterDeserializer.deserialize(envelope.value);
  }

  @Override
  protected V getValueRecord(ComputeResponseRecordV1 envelope, CompressionStrategy compression) {
    if (!envelope.value.hasRemaining()) {
      // Safeguard to handle empty value, which indicates non-existing key.
      return null;
    }
    if (!compression.equals(CompressionStrategy.NO_OP)) {
      throw new VeniceClientException("Unexpected compression: " + compression);
    }
    return valueDeserializer.deserialize(envelope.value);
  }

  @Override
  protected int getKeyIndex(ComputeResponseRecordV1 envelope) {
    return envelope.getKeyIndex();
  }
}
