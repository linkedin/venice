package com.linkedin.venice.client.store.streaming;

import static com.linkedin.venice.streaming.StreamingConstants.KEY_ID_FOR_STREAMING_FOOTER;

import com.linkedin.venice.HttpConstants;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.exceptions.VeniceClientHttpException;
import com.linkedin.venice.compression.CompressionStrategy;
import com.linkedin.venice.read.protocol.response.streaming.StreamingFooterRecordV1;
import com.linkedin.venice.schema.SchemaData;
import com.linkedin.venice.utils.ByteUtils;
import com.linkedin.venice.utils.LatencyUtils;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;


/**
 * Streaming callback for batch-get/compute.
 *
 * Since data chunk returned by {@link D2TransportClient} doesn't respect chunk-size associated with each
 * chunk sent by Venice Router, here is leveraging {@link ReadEnvelopeChunkedDeserializer} to deserialize
 * data chunks even with the data chunk contains partial record in a non-blocking way.
 *
 * The envelope deserialization will happen in TransportClient thread pool (R2 thread pool for example if using
 * {@link D2TransportClient}, and both the record deserialization and application's callback will be executed in
 * Venice thread pool: {@link #deserializationExecutor},
 *
 * @param <ENVELOPE>
 * @param <K>
 * @param <V>
 */
public abstract class AbstractRecordStreamDecoder<ENVELOPE, K, V> implements RecordStreamDecoder {
  private final List<K> keyList;
  private final TrackingStreamingCallback<K, V> callback;
  private final List<CompletableFuture<Void>> deserializationFutures = new ArrayList<>();
  private final BitSet receivedKeySet; // Track which key is present in the response
  private final AtomicInteger successfulKeyCnt = new AtomicInteger(0);
  private final long preSubmitTimeInNS;
  private final LongAdder deserializationTimeInNS = new LongAdder();
  private final Executor deserializationExecutor;

  // non-final state
  private int duplicateEntryCount = 0;
  private Optional<StreamingFooterRecordV1> streamingFooterRecord = Optional.empty();
  private boolean isStreamingResponse = false;
  private int responseSchemaId = SchemaData.INVALID_VALUE_SCHEMA_ID;
  private CompressionStrategy compressionStrategy = CompressionStrategy.NO_OP;
  private ReadEnvelopeChunkedDeserializer<ENVELOPE> envelopeDeserializer = null;

  public AbstractRecordStreamDecoder(
      List<K> keyList,
      TrackingStreamingCallback<K, V> callback,
      Executor deserializationExecutor) {
    this.keyList = keyList;
    this.callback = callback;
    this.deserializationExecutor = deserializationExecutor;
    this.preSubmitTimeInNS = System.nanoTime();
    this.receivedKeySet = new BitSet(keyList.size());
  }

  @Override
  public void onHeaderReceived(Map<String, String> headers) {
    callback.getStats()
        .ifPresent(
            stats -> stats
                .recordRequestSubmissionToResponseHandlingTime(LatencyUtils.getLatencyInMS(preSubmitTimeInNS)));
    isStreamingResponse = headers.containsKey(HttpConstants.VENICE_STREAMING_RESPONSE);
    String schemaIdHeader = headers.get(HttpConstants.VENICE_SCHEMA_ID);
    if (schemaIdHeader != null) {
      responseSchemaId = Integer.parseInt(schemaIdHeader);
    }
    envelopeDeserializer = getEnvelopeDeserializer(responseSchemaId);
    String compressionHeader = headers.get(HttpConstants.VENICE_COMPRESSION_STRATEGY);
    if (compressionHeader != null) {
      compressionStrategy = CompressionStrategy.valueOf(Integer.parseInt(compressionHeader));
    }
  }

  private void validateKeyIdx(int keyIdx) {
    if (KEY_ID_FOR_STREAMING_FOOTER == keyIdx) {
      // footer record
      return;
    }
    final int absKeyIdx = Math.abs(keyIdx);
    if (absKeyIdx < keyList.size()) {
      return;
    }
    throw new VeniceClientException(
        "Invalid key index: " + keyIdx + ", either it should be the footer" + " record key index: "
            + KEY_ID_FOR_STREAMING_FOOTER + " or its absolute value should be [0, " + keyList.size() + ")");
  }

  @Override
  public void onDataReceived(ByteBuffer chunk) {
    if (envelopeDeserializer == null) {
      throw new VeniceClientException("Envelope deserializer hasn't been initialized yet");
    }
    envelopeDeserializer.write(chunk);
    // Envelope deserialization has to happen sequentially
    final List<ENVELOPE> availableRecords = envelopeDeserializer.consume();
    if (availableRecords.isEmpty()) {
      // no full record is available
      return;
    }
    CompletableFuture<Void> deserializationFuture = CompletableFuture.runAsync(() -> {
      Map<K, V> resultMap = new HashMap<>();
      for (ENVELOPE record: availableRecords) {
        final int keyIdx = getKeyIndex(record);
        validateKeyIdx(keyIdx);
        if (KEY_ID_FOR_STREAMING_FOOTER == keyIdx) {
          // Deserialize footer record
          streamingFooterRecord = Optional.of(getStreamingFooterRecord(record));
          break;
        }
        final int absKeyIdx = Math.abs(keyIdx);
        // Track duplicate entries per request
        if (absKeyIdx < keyList.size()) {
          synchronized (receivedKeySet) {
            if (receivedKeySet.get(absKeyIdx)) {
              // Encounter duplicate entry because of retrying logic in Venice Router
              ++duplicateEntryCount;
              continue;
            }
            receivedKeySet.set(absKeyIdx);
          }
        }
        K key = keyList.get(absKeyIdx);

        V value;
        if (keyIdx < 0) {
          // Key doesn't exist
          value = null;
        } else {
          /**
           * The above condition could NOT capture the non-existing key with index: 0,
           * so {@link DeserializerFunc#deserialize(Object, CompressionStrategy)} needs to handle it by checking
           * whether the value is an empty byte array or not, and essentially the deserialization function should
           * return null in this situation.
           */
          long preRecordDeserializationInNS = System.nanoTime();
          value = getValueRecord(record, compressionStrategy);
          deserializationTimeInNS.add(System.nanoTime() - preRecordDeserializationInNS);
          /**
           * If key index is not 0, it is unexpected to receive non-null value.
           */
          if (value == null && keyIdx != 0) {
            throw new VeniceClientException("Expected to receive non-null value for key: " + keyList.get(keyIdx));
          }
        }
        callback.onRecordDeserialized();
        resultMap.put(key, value);
        if (value != null) {
          successfulKeyCnt.incrementAndGet();
        }
      }
      if (resultMap.isEmpty()) {
        return;
      }
      /**
       * Execute the user callback in the same thread.
       *
       * There is a bug in JDK8, which could cause {@link CompletableFuture#allOf(CompletableFuture[])} if there
       * are multiple layers of async processing:
       * https://bugs.openjdk.java.net/browse/JDK-8201576
       * So if the user's callback is executed in another async handler, {@link CompletableFuture#allOf(CompletableFuture[])}
       * will hang sometimes.
       * Also with this way, the context switches are also reduced.
        */
      resultMap.forEach(callback::onRecordReceived);
    }, deserializationExecutor);
    deserializationFutures.add(deserializationFuture);
  }

  @Override
  public void onCompletion(Optional<VeniceClientException> exception) {
    // Only complete it when all the futures are done.
    CompletableFuture.allOf(deserializationFutures.toArray(new CompletableFuture[0]))
        .whenComplete((voidP, throwable) -> {
          Optional<Exception> completedException = Optional.empty();
          if (exception.isPresent()) {
            // Exception thrown by transporting layer
            completedException = Optional.of(exception.get());
          } else if (streamingFooterRecord.isPresent()) {
            // Exception thrown by Venice backend
            completedException = Optional.of(
                new VeniceClientHttpException(
                    new String(ByteUtils.extractByteArray(streamingFooterRecord.get().detail)),
                    streamingFooterRecord.get().status));
          } else if (throwable != null) {
            if (throwable instanceof Exception) {
              completedException = Optional.of((Exception) throwable);
            } else {
              completedException = Optional.of(new Exception(throwable));
            }
          } else {
            // Everything is good
            if (!isStreamingResponse) {
              /**
               * For regular response, the non-existing keys won't be present in the response,
               * so we need to manually collect the non-existing keys and trigger customer's callback.
               */
              for (int i = 0; i < keyList.size(); ++i) {
                if (!receivedKeySet.get(i)) {
                  callback.onRecordReceived(keyList.get(i), null);
                  callback.onRecordDeserialized();
                  /**
                   * To match the streaming response, we will mark the non-existing keys as received as well.
                   */
                  receivedKeySet.set(i);
                }
              }
            }
          }
          callback.onCompletion(completedException);
          callback.onDeserializationCompletion(completedException, successfulKeyCnt.get(), duplicateEntryCount);
          callback.getStats()
              .ifPresent(
                  stats -> stats.recordResponseDeserializationTime(
                      LatencyUtils.convertLatencyFromNSToMS(deserializationTimeInNS.sum())));
        });
  }

  protected abstract ReadEnvelopeChunkedDeserializer<ENVELOPE> getEnvelopeDeserializer(int schemaId);

  protected abstract StreamingFooterRecordV1 getStreamingFooterRecord(ENVELOPE envelope);

  protected abstract V getValueRecord(ENVELOPE envelope, CompressionStrategy compression);

  protected abstract int getKeyIndex(ENVELOPE envelope);
}
