package com.linkedin.venice.hadoop.task.datawriter;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import static org.testng.Assert.fail;

import com.linkedin.venice.pubsub.api.PubSubProduceResult;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.AbstractVeniceWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.testng.annotations.Test;


public class DualWriteVeniceWriterTest {
  private static final String TOPIC = "test_store_v1";
  private static final byte[] KEY_PREFIX = "key_".getBytes();
  private static final byte[] VALUE_PREFIX = "value_".getBytes();
  private static final int SCHEMA_ID = 7;

  @Test
  public void rejectsBatchSizeLessThanOne() {
    AbstractVeniceWriter<byte[], byte[], byte[]> kafkaWriter = mock(AbstractVeniceWriter.class);
    RecordingExternalStorageWriter external = new RecordingExternalStorageWriter();
    assertThrows(IllegalArgumentException.class, () -> new DualWriteVeniceWriter(TOPIC, kafkaWriter, external, 0));
  }

  @Test
  public void batchSizeOneFlushesEveryRecordImmediately() throws IOException {
    AbstractVeniceWriter<byte[], byte[], byte[]> kafkaWriter = mockKafkaWriter();
    RecordingExternalStorageWriter external = new RecordingExternalStorageWriter();
    try (DualWriteVeniceWriter writer = new DualWriteVeniceWriter(TOPIC, kafkaWriter, external, 1)) {
      writer.put(key(1), value(1), SCHEMA_ID, null);
      assertEquals(external.batchPutInvocations.size(), 1, "Per-record flush expected at batchSize=1");
      assertEquals(external.batchPutInvocations.get(0).size(), 1);

      writer.put(key(2), value(2), SCHEMA_ID, null);
      assertEquals(external.batchPutInvocations.size(), 2);
      assertEquals(external.batchPutInvocations.get(1).size(), 1);
      assertEquals(writer.getBufferedPutCount(), 0, "Buffer must be empty at batchSize=1");
    }
  }

  @Test
  public void batchSizeNAccumulatesUntilThreshold() throws IOException {
    AbstractVeniceWriter<byte[], byte[], byte[]> kafkaWriter = mockKafkaWriter();
    RecordingExternalStorageWriter external = new RecordingExternalStorageWriter();
    try (DualWriteVeniceWriter writer = new DualWriteVeniceWriter(TOPIC, kafkaWriter, external, 5)) {
      for (int i = 1; i <= 4; i++) {
        writer.put(key(i), value(i), SCHEMA_ID, null);
      }
      assertEquals(external.batchPutInvocations.size(), 0, "No flush expected before threshold");
      assertEquals(writer.getBufferedPutCount(), 4);

      writer.put(key(5), value(5), SCHEMA_ID, null);
      assertEquals(external.batchPutInvocations.size(), 1, "Threshold should trigger a flush");
      assertEquals(external.batchPutInvocations.get(0).size(), 5);
      assertEquals(writer.getBufferedPutCount(), 0);

      // Second full batch
      for (int i = 6; i <= 10; i++) {
        writer.put(key(i), value(i), SCHEMA_ID, null);
      }
      assertEquals(external.batchPutInvocations.size(), 2);
      assertEquals(external.batchPutInvocations.get(1).size(), 5);
    }
  }

  @Test
  public void partialBatchDrainedOnFlush() throws IOException {
    AbstractVeniceWriter<byte[], byte[], byte[]> kafkaWriter = mockKafkaWriter();
    RecordingExternalStorageWriter external = new RecordingExternalStorageWriter();
    try (DualWriteVeniceWriter writer = new DualWriteVeniceWriter(TOPIC, kafkaWriter, external, 10)) {
      for (int i = 1; i <= 3; i++) {
        writer.put(key(i), value(i), SCHEMA_ID, null);
      }
      assertEquals(external.batchPutInvocations.size(), 0);
      writer.flush();
      assertEquals(external.batchPutInvocations.size(), 1);
      assertEquals(external.batchPutInvocations.get(0).size(), 3, "Partial batch should drain on flush");
      assertTrue(external.flushCount > 0, "flush() should also call externalWriter.flush()");
    }
  }

  @Test
  public void closeDrainsBufferAndClosesBothSides() throws IOException {
    AbstractVeniceWriter<byte[], byte[], byte[]> kafkaWriter = mockKafkaWriter();
    RecordingExternalStorageWriter external = new RecordingExternalStorageWriter();
    DualWriteVeniceWriter writer = new DualWriteVeniceWriter(TOPIC, kafkaWriter, external, 100);
    writer.put(key(1), value(1), SCHEMA_ID, null);
    writer.put(key(2), value(2), SCHEMA_ID, null);
    writer.close();

    assertEquals(external.batchPutInvocations.size(), 1, "close() should drain via flush() before closing");
    assertEquals(external.batchPutInvocations.get(0).size(), 2);
    assertTrue(external.closed, "external.close() must be called");
  }

  @Test
  public void putFutureCompletesAfterDrain() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> kafkaWriter = mockKafkaWriter();
    RecordingExternalStorageWriter external = new RecordingExternalStorageWriter();
    try (DualWriteVeniceWriter writer = new DualWriteVeniceWriter(TOPIC, kafkaWriter, external, 3)) {
      CompletableFuture<PubSubProduceResult> first = writer.put(key(1), value(1), SCHEMA_ID, null);
      CompletableFuture<PubSubProduceResult> second = writer.put(key(2), value(2), SCHEMA_ID, null);
      assertFalse(first.isDone(), "Buffered put future should not complete until drain");
      assertFalse(second.isDone());

      writer.flush();
      assertTrue(first.isDone(), "Drain via flush should complete the buffered put's future");
      assertTrue(second.isDone());
    }
  }

  @Test
  public void updateIsNotSupported() throws IOException {
    AbstractVeniceWriter<byte[], byte[], byte[]> kafkaWriter = mockKafkaWriter();
    RecordingExternalStorageWriter external = new RecordingExternalStorageWriter();
    try (DualWriteVeniceWriter writer = new DualWriteVeniceWriter(TOPIC, kafkaWriter, external, 1)) {
      assertThrows(UnsupportedOperationException.class, () -> writer.update(key(1), value(1), SCHEMA_ID, -1, null));
    }
  }

  @Test
  public void deleteIsNotSupported() throws IOException {
    AbstractVeniceWriter<byte[], byte[], byte[]> kafkaWriter = mockKafkaWriter();
    RecordingExternalStorageWriter external = new RecordingExternalStorageWriter();
    try (DualWriteVeniceWriter writer = new DualWriteVeniceWriter(TOPIC, kafkaWriter, external, 1)) {
      assertThrows(UnsupportedOperationException.class, () -> writer.delete(key(1), null));
    }
  }

  @Test
  public void closeSelfFlushesDrainsBufferAndFlushesBothInnerWriters() throws IOException {
    AbstractVeniceWriter<byte[], byte[], byte[]> kafkaWriter = mockKafkaWriter();
    RecordingExternalStorageWriter external = new RecordingExternalStorageWriter();
    DualWriteVeniceWriter writer = new DualWriteVeniceWriter(TOPIC, kafkaWriter, external, 100);
    writer.put(key(1), value(1), SCHEMA_ID, null);
    writer.put(key(2), value(2), SCHEMA_ID, null);
    // Intentionally skip calling writer.flush(); the contract says close() alone is sufficient.
    writer.close();

    assertEquals(
        external.batchPutInvocations.size(),
        1,
        "close() must drain pending puts via the buffer before closing");
    assertEquals(external.batchPutInvocations.get(0).size(), 2);
    assertTrue(
        external.flushCount > 0,
        "close() must call externalWriter.flush() so impls with internal buffers drain");
    assertTrue(external.closed, "externalWriter.close() must still run after the self-flush");
  }

  @Test
  public void valueIsRocksDbFormattedWithBigEndianSchemaIdPrefix() throws IOException {
    AbstractVeniceWriter<byte[], byte[], byte[]> kafkaWriter = mockKafkaWriter();
    RecordingExternalStorageWriter external = new RecordingExternalStorageWriter();
    byte[] rawValue = value(42);
    try (DualWriteVeniceWriter writer = new DualWriteVeniceWriter(TOPIC, kafkaWriter, external, 1)) {
      writer.put(key(42), rawValue, SCHEMA_ID, null);
    }

    assertEquals(external.batchPutInvocations.size(), 1);
    assertEquals(external.batchPutInvocations.get(0).size(), 1);
    ExternalStorageRecord forwarded = external.batchPutInvocations.get(0).get(0);
    byte[] formattedValue = forwarded.getValue();
    assertEquals(
        formattedValue.length,
        ExternalStorageRecord.SCHEMA_ID_PREFIX_LENGTH + rawValue.length,
        "Forwarded value should be schema-id prefix followed by the raw value bytes");
    ByteBuffer view = ByteBuffer.wrap(formattedValue);
    assertEquals(view.getInt(), SCHEMA_ID, "First 4 bytes must be the BE-encoded value schema id");
    byte[] payload = new byte[view.remaining()];
    view.get(payload);
    assertEquals(payload, rawValue, "Bytes after the prefix must equal the original value payload");
  }

  // --- Retry tests ---------------------------------------------------------------------------------

  @Test
  public void rejectsNegativeBatchPutRetries() {
    AbstractVeniceWriter<byte[], byte[], byte[]> kafkaWriter = mock(AbstractVeniceWriter.class);
    RecordingExternalStorageWriter external = new RecordingExternalStorageWriter();
    assertThrows(
        IllegalArgumentException.class,
        () -> new DualWriteVeniceWriter(TOPIC, kafkaWriter, external, 1, -1, 0));
  }

  @Test
  public void rejectsNegativeBatchPutRetryBackoff() {
    AbstractVeniceWriter<byte[], byte[], byte[]> kafkaWriter = mock(AbstractVeniceWriter.class);
    RecordingExternalStorageWriter external = new RecordingExternalStorageWriter();
    assertThrows(
        IllegalArgumentException.class,
        () -> new DualWriteVeniceWriter(TOPIC, kafkaWriter, external, 1, 3, -1));
  }

  /**
   * Transient external failure: external throws on attempts 1 and 2, succeeds on attempt 3. With
   * {@code batchPutRetries=3} (4 attempts total), the wrapper recovers without falling through to
   * Spark's whole-partition retry, and the corresponding Kafka produce eventually fires.
   */
  @Test
  public void recoversAfterTransientExternalFailureWithinRetryBudget() throws IOException {
    AbstractVeniceWriter<byte[], byte[], byte[]> kafkaWriter = mockKafkaWriter();
    RecordingExternalStorageWriter external = new RecordingExternalStorageWriter();
    external.failBatchPutTimes = 2; // succeed on the 3rd attempt
    try (DualWriteVeniceWriter writer = new DualWriteVeniceWriter(TOPIC, kafkaWriter, external, 1, 3, 0)) {
      writer.put(key(1), value(1), SCHEMA_ID, null);
      assertEquals(external.batchPutAttempts, 3, "Expected 3 attempts: 2 failures + 1 success");
      assertEquals(
          external.batchPutInvocations.size(),
          1,
          "Only the successful attempt should add to the recorded batchPut history");
      verify(kafkaWriter).put(any(), any(), anyInt(), any());
    }
  }

  /**
   * External keeps failing past the retry budget. Total attempts = {@code batchPutRetries + 1}; the
   * original failure propagates and the later failures are attached via {@code getSuppressed()}.
   */
  @Test
  public void exhaustingRetriesPropagatesOriginalFailureWithSuppressedRetries() throws IOException {
    AbstractVeniceWriter<byte[], byte[], byte[]> kafkaWriter = mockKafkaWriter();
    RecordingExternalStorageWriter external = new RecordingExternalStorageWriter();
    external.failBatchPutTimes = Integer.MAX_VALUE; // never succeed
    DualWriteVeniceWriter writer = new DualWriteVeniceWriter(TOPIC, kafkaWriter, external, 1, 3, 0);
    try {
      RuntimeException raised =
          expectThrows(RuntimeException.class, () -> writer.put(key(1), value(1), SCHEMA_ID, null));
      assertEquals(external.batchPutAttempts, 4, "Expected 1 initial + 3 retries = 4 total attempts");
      assertTrue(
          raised.getMessage().contains("simulated batchPut failure #1"),
          "First failure should be the propagating exception; got: " + raised.getMessage());
      Throwable[] suppressed = raised.getSuppressed();
      assertEquals(suppressed.length, 3, "Subsequent 3 failures should be attached via addSuppressed");
      assertTrue(suppressed[0].getMessage().contains("simulated batchPut failure #2"));
      assertTrue(suppressed[1].getMessage().contains("simulated batchPut failure #3"));
      assertTrue(suppressed[2].getMessage().contains("simulated batchPut failure #4"));
      verify(kafkaWriter, never()).put(any(), any(), anyInt(), any());
    } finally {
      try {
        writer.close();
      } catch (Exception ignored) {
      }
    }
  }

  // --- Failure-mode tests --------------------------------------------------------------------------

  /**
   * I4 (1): external-throw at the {@code batchPut} boundary fails the put synchronously AND the wrapper
   * never invokes any {@code kafkaWriter.put} for the failed batch. Protects the external-first ordering
   * invariant: a regression that silently produced to Kafka on external failure would mean external sink
   * and Venice diverge.
   */
  @Test
  public void externalBatchPutFailureBlocksKafkaProduce() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> kafkaWriter = mockKafkaWriter();
    RecordingExternalStorageWriter external = new RecordingExternalStorageWriter();
    external.throwOnBatchPut = new RuntimeException("simulated external storage failure");
    DualWriteVeniceWriter writer = new DualWriteVeniceWriter(TOPIC, kafkaWriter, external, 1);
    try {
      RuntimeException raised =
          expectThrows(RuntimeException.class, () -> writer.put(key(1), value(1), SCHEMA_ID, null));
      assertTrue(
          raised.getMessage().contains("simulated external storage failure"),
          "Caller should see the original failure; got: " + raised.getMessage());
      verify(kafkaWriter, never()).put(any(), any(), anyInt(), any());
      verify(kafkaWriter, never()).put(any(), any(), anyInt(), any(), any());
      assertEquals(writer.getBufferedPutCount(), 0, "Buffer must be cleared even on failure path");
    } finally {
      // Close path: flush() will try to drainBuffer again, but buffer is empty so it's a no-op. external
      // throwOnBatchPut is still set but no batchPut happens since the buffer is empty.
      try {
        writer.close();
      } catch (Exception ignored) {
        // Close may surface lingering errors from earlier; tolerate here — the assertions above are what
        // we care about.
      }
    }
  }

  /**
   * I4 (2): a failing Kafka future surfaces as exceptional completion on the buffered put's future,
   * which is how Spark detects task failure for buffered writes.
   */
  @Test
  public void kafkaFutureFailurePropagatesToBufferedPutFuture() throws Exception {
    AbstractVeniceWriter<byte[], byte[], byte[]> kafkaWriter = mock(AbstractVeniceWriter.class);
    RuntimeException kafkaError = new RuntimeException("simulated Kafka produce failure");
    CompletableFuture<PubSubProduceResult> failed = new CompletableFuture<>();
    failed.completeExceptionally(kafkaError);
    doReturn(failed).when(kafkaWriter).put(any(), any(), anyInt(), any());
    doReturn(CompletableFuture.completedFuture(null)).when(kafkaWriter).put(any(), any(), anyInt(), any(), any());

    RecordingExternalStorageWriter external = new RecordingExternalStorageWriter();
    try (DualWriteVeniceWriter writer = new DualWriteVeniceWriter(TOPIC, kafkaWriter, external, 3)) {
      CompletableFuture<PubSubProduceResult> first = writer.put(key(1), value(1), SCHEMA_ID, null);
      writer.put(key(2), value(2), SCHEMA_ID, null);
      writer.flush();

      assertTrue(first.isDone());
      ExecutionException ee = expectThrows(ExecutionException.class, first::get);
      assertTrue(
          ee.getCause().getMessage().contains("simulated Kafka produce failure"),
          "Buffered put's future should carry the Kafka failure cause; got: " + ee.getCause().getMessage());
    }
  }

  /**
   * I4 (3): when both inner close calls throw, the first IOException propagates and the second is
   * attached via {@code getSuppressed()}. Protects against silent loss of diagnostics during executor
   * shutdown when both Kafka and the external sink can fail concurrently (e.g. network partition).
   */
  @Test
  public void closeMultiFailureSuppression() throws IOException {
    AbstractVeniceWriter<byte[], byte[], byte[]> kafkaWriter = mockKafkaWriter();
    IOException kafkaCloseError = new IOException("kafka close exploded");
    doThrow(kafkaCloseError).when(kafkaWriter).close(true);

    RecordingExternalStorageWriter external = new RecordingExternalStorageWriter();
    external.throwOnClose = new IOException("external close exploded");

    DualWriteVeniceWriter writer = new DualWriteVeniceWriter(TOPIC, kafkaWriter, external, 1);
    try {
      writer.close();
      fail("Expected IOException from one of the two failing close calls");
    } catch (IOException raised) {
      assertEquals(
          raised.getMessage(),
          "external close exploded",
          "First reported error should be the externalWriter close (called before kafkaWriter close)");
      Throwable[] suppressed = raised.getSuppressed();
      assertEquals(suppressed.length, 1, "Kafka close's IOException should be attached via addSuppressed");
      assertEquals(suppressed[0].getMessage(), "kafka close exploded");
    }
  }

  // --- Per-region fan-out tests --------------------------------------------------------------------

  @Test
  public void rejectsEmptyExternalWriterList() {
    AbstractVeniceWriter<byte[], byte[], byte[]> kafkaWriter = mock(AbstractVeniceWriter.class);
    assertThrows(
        IllegalArgumentException.class,
        () -> new DualWriteVeniceWriter(TOPIC, kafkaWriter, new ArrayList<>(), 1, 0, 0L));
  }

  @Test
  public void fansOutEveryRecordToAllRegionalWriters() throws IOException {
    AbstractVeniceWriter<byte[], byte[], byte[]> kafkaWriter = mockKafkaWriter();
    RecordingExternalStorageWriter dc0 = new RecordingExternalStorageWriter();
    RecordingExternalStorageWriter dc1 = new RecordingExternalStorageWriter();
    try (DualWriteVeniceWriter writer =
        new DualWriteVeniceWriter(TOPIC, kafkaWriter, Arrays.asList(dc0, dc1), 1, 0, 0L)) {
      writer.put(key(1), value(1), SCHEMA_ID, null);
      writer.put(key(2), value(2), SCHEMA_ID, null);
    }
    // Each regional writer received both records; Kafka produced each record exactly once (not per region).
    for (RecordingExternalStorageWriter region: Arrays.asList(dc0, dc1)) {
      assertEquals(region.batchPutInvocations.size(), 2, "Each region should see both per-record batchPuts");
      assertTrue(region.flushCount > 0, "Each regional writer should be flushed");
      assertTrue(region.closed, "Each regional writer should be closed");
    }
    verify(kafkaWriter, times(2)).put(any(), any(), anyInt(), any());
  }

  @Test
  public void failureInOneRegionalWriterFailsThePushAndBlocksKafka() throws IOException {
    AbstractVeniceWriter<byte[], byte[], byte[]> kafkaWriter = mockKafkaWriter();
    RecordingExternalStorageWriter healthy = new RecordingExternalStorageWriter();
    RecordingExternalStorageWriter failing = new RecordingExternalStorageWriter();
    failing.throwOnBatchPut = new RuntimeException("simulated regional sink failure");
    DualWriteVeniceWriter writer =
        new DualWriteVeniceWriter(TOPIC, kafkaWriter, Arrays.asList(healthy, failing), 1, 0, 0L);
    try {
      RuntimeException raised =
          expectThrows(RuntimeException.class, () -> writer.put(key(1), value(1), SCHEMA_ID, null));
      assertTrue(
          raised.getMessage().contains("simulated regional sink failure"),
          "A regional sink failure should fail the push; got: " + raised.getMessage());
      verify(kafkaWriter, never()).put(any(), any(), anyInt(), any());
    } finally {
      // The injected failure is on batchPut, not close, so close() completes normally here; any IOException
      // it did raise would propagate via the method's throws clause rather than mask the assertions above.
      writer.close();
    }
  }

  private static AbstractVeniceWriter<byte[], byte[], byte[]> mockKafkaWriter() {
    @SuppressWarnings("unchecked")
    AbstractVeniceWriter<byte[], byte[], byte[]> kafkaWriter = mock(AbstractVeniceWriter.class);
    CompletableFuture<PubSubProduceResult> completed = CompletableFuture.completedFuture(null);
    // any() (no class) matches null callbacks; any(Class) does not.
    doReturn(completed).when(kafkaWriter).put(any(), any(), anyInt(), any());
    doReturn(completed).when(kafkaWriter).put(any(), any(), anyInt(), any(), any());
    return kafkaWriter;
  }

  private static byte[] key(int i) {
    return concat(KEY_PREFIX, Integer.toString(i).getBytes());
  }

  private static byte[] value(int i) {
    return concat(VALUE_PREFIX, Integer.toString(i).getBytes());
  }

  private static byte[] concat(byte[] a, byte[] b) {
    byte[] out = new byte[a.length + b.length];
    System.arraycopy(a, 0, out, 0, a.length);
    System.arraycopy(b, 0, out, a.length, b.length);
    return out;
  }

  /**
   * Minimal recording impl of {@link ExternalStorageWriter} for unit-test assertions. Captures every
   * {@code batchPut} invocation as a separate list snapshot. Flags allow per-test injection of failures.
   */
  private static final class RecordingExternalStorageWriter implements ExternalStorageWriter {
    final List<List<ExternalStorageRecord>> batchPutInvocations = new ArrayList<>();
    int flushCount = 0;
    int batchPutAttempts = 0;
    boolean closed = false;
    RuntimeException throwOnBatchPut = null;
    /** Number of leading {@code batchPut} attempts that should throw before the first success. */
    int failBatchPutTimes = 0;
    IOException throwOnClose = null;

    @Override
    public void configure(VeniceProperties jobProps, String topicName, int partitionId) {
    }

    @Override
    public void batchPut(List<ExternalStorageRecord> records) {
      batchPutAttempts++;
      if (throwOnBatchPut != null) {
        throw throwOnBatchPut;
      }
      if (batchPutAttempts <= failBatchPutTimes) {
        throw new RuntimeException("simulated batchPut failure #" + batchPutAttempts);
      }
      batchPutInvocations.add(new ArrayList<>(records));
    }

    @Override
    public void flush() {
      flushCount++;
    }

    @Override
    public void close() throws IOException {
      closed = true;
      if (throwOnClose != null) {
        throw throwOnClose;
      }
    }
  }
}
