package com.linkedin.venice.listener.response;

import com.linkedin.davinci.listener.response.ReadResponseStats;
import com.linkedin.venice.listener.response.stats.MultiKeyResponseStats;
import com.linkedin.venice.listener.response.stats.ReadResponseStatsRecorder;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.utils.lazy.Lazy;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public abstract class MultiKeyResponseWrapper<K> extends AbstractReadResponse {
  private final MultiKeyResponseStats responseStats;
  private final Lazy<ByteBuf> responseBody;

  /**
   * Mutable because we want {@link #getResponseBody()} to "freeze" this list.
   */
  private List<K> records;

  MultiKeyResponseWrapper(int maxKeyCount, MultiKeyResponseStats responseStats, RecordSerializer<K> recordSerializer) {
    this.records = new ArrayList<>(maxKeyCount);
    this.responseStats = responseStats;
    this.responseBody = Lazy.of(() -> {
      ByteBuf responseBodyByteBuf = Unpooled.wrappedBuffer(recordSerializer.serializeObjects(records));
      this.responseStats.setRecordCount(this.records.size());

      /**
       * This re-assignment prevents additional records from being added after the response body is generated.
       * It can also potentially help GC by allowing these references which are no longer needed to be collected sooner.
       */
      this.records = Collections.emptyList();
      return responseBodyByteBuf;
    });
  }

  /**
   * @param record to be added into the container.
   * @throws IllegalStateException if called after {@link #getResponseBody()}
   */
  public void addRecord(K record) {
    try {
      records.add(record);
    } catch (UnsupportedOperationException e) {
      // Defensive code, should never happen unless we have a regression.
      throw new IllegalStateException(
          this.getClass().getSimpleName() + ".addRecord() cannot be called after getResponseBody().");
    }
  }

  public abstract int getResponseSchemaIdHeader();

  @Override
  public ReadResponseStats getStats() {
    return this.responseStats;
  }

  @Override
  public ReadResponseStatsRecorder getStatsRecorder() {
    return this.responseStats;
  }

  /**
   * N.B.: This function is backed by a {@link Lazy} property. The first time it is called, the response is recorded and
   * will therefore not be regenerated even if the more records are added. To make this behavior unambiguous, it is no
   * longer possible to add records after calling this function, as the {@link #records} property then becomes an empty
   * list.
   *
   * @return the serialized response as a {@link ByteBuf}
   */
  @Override
  public ByteBuf getResponseBody() {
    return responseBody.get();
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName() + "(" + this.records.size() + " records)";
  }
}
