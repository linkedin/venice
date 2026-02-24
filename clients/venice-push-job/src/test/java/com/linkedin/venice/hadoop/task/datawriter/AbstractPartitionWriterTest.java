package com.linkedin.venice.hadoop.task.datawriter;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;
import org.testng.annotations.Test;


public class AbstractPartitionWriterTest {
  private static final byte[] DUMMY_DATA = new byte[] { 0, 1, 2 };
  private static final byte[] EMPTY_BYTES = new byte[0];

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Found replication metadata without a valid schema id")
  public void testInvalidRmdSchemaWithValidRmdPayload() {
    AbstractPartitionWriter.VeniceWriterMessage message = new AbstractPartitionWriter.VeniceWriterMessage(
        DUMMY_DATA,
        DUMMY_DATA,
        1,
        -1,
        ByteBuffer.wrap(DUMMY_DATA),
        mock(PubSubProducerCallback.class),
        false,
        1);
    message.getConsumer().accept(mock(VeniceWriter.class));
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Found empty replication metadata")
  public void testInvalidRmdSchemaWithInvalidRmdPayload() {
    AbstractPartitionWriter.VeniceWriterMessage message = new AbstractPartitionWriter.VeniceWriterMessage(
        DUMMY_DATA,
        DUMMY_DATA,
        1,
        -1,
        ByteBuffer.wrap(EMPTY_BYTES),
        mock(PubSubProducerCallback.class),
        false,
        1);
    message.getConsumer().accept(mock(VeniceWriter.class));
  }

  @Test
  public void testExtractNormalRecord() throws Exception {
    // Normal record with non-empty value and RMD, default schema IDs → uses global fallback
    TestablePartitionWriter writer = createConfiguredWriter(10, 5);

    byte[] keyBytes = "test-key".getBytes();
    byte[] valueBytes = "test-value".getBytes();
    byte[] rmdBytes = "test-rmd".getBytes();

    AbstractPartitionWriter.VeniceRecordWithMetadata record =
        new AbstractPartitionWriter.VeniceRecordWithMetadata(valueBytes, rmdBytes);
    Iterator<AbstractPartitionWriter.VeniceRecordWithMetadata> values = Collections.singletonList(record).iterator();

    AbstractPartitionWriter.VeniceWriterMessage message =
        writer.testExtract(keyBytes, values, mock(DataWriterTaskTracker.class));

    assertNotNull(message);
    assertEquals(message.getKeyBytes(), keyBytes);
    assertEquals(message.getValueBytes(), valueBytes);
    assertEquals(message.getValueSchemaId(), 10);
    assertEquals(message.getRmdVersionId(), 5);
  }

  @Test
  public void testExtractPerRecordSchemaIds() throws Exception {
    TestablePartitionWriter writer = createConfiguredWriter(10, 5);

    byte[] keyBytes = "test-key".getBytes();
    byte[] valueBytes = "test-value".getBytes();
    byte[] rmdBytes = "test-rmd".getBytes();

    AbstractPartitionWriter.VeniceRecordWithMetadata record =
        new AbstractPartitionWriter.VeniceRecordWithMetadata(valueBytes, rmdBytes, 42, 7);
    Iterator<AbstractPartitionWriter.VeniceRecordWithMetadata> values = Collections.singletonList(record).iterator();

    AbstractPartitionWriter.VeniceWriterMessage message =
        writer.testExtract(keyBytes, values, mock(DataWriterTaskTracker.class));

    assertNotNull(message);
    assertEquals(message.getValueSchemaId(), 42);
    assertEquals(message.getRmdVersionId(), 7);
  }

  @Test
  public void testExtractNullValueNullRmdDropped() throws Exception {
    TestablePartitionWriter writer = createConfiguredWriter(10, 5);

    byte[] keyBytes = "test-key".getBytes();

    AbstractPartitionWriter.VeniceRecordWithMetadata record =
        new AbstractPartitionWriter.VeniceRecordWithMetadata(null, null);
    Iterator<AbstractPartitionWriter.VeniceRecordWithMetadata> values = Collections.singletonList(record).iterator();

    AbstractPartitionWriter.VeniceWriterMessage message =
        writer.testExtract(keyBytes, values, mock(DataWriterTaskTracker.class));

    assertNull(message);
  }

  @Test
  public void testExtractEmptyRmdTreatedAsNull() throws Exception {
    // Empty RMD → treated as null. Non-null value → record is kept.
    TestablePartitionWriter writer = createConfiguredWriter(10, 5);

    byte[] keyBytes = "test-key".getBytes();
    byte[] valueBytes = "test-value".getBytes();
    byte[] emptyRmd = new byte[0];

    AbstractPartitionWriter.VeniceRecordWithMetadata record =
        new AbstractPartitionWriter.VeniceRecordWithMetadata(valueBytes, emptyRmd);
    Iterator<AbstractPartitionWriter.VeniceRecordWithMetadata> values = Collections.singletonList(record).iterator();

    AbstractPartitionWriter.VeniceWriterMessage message =
        writer.testExtract(keyBytes, values, mock(DataWriterTaskTracker.class));

    assertNotNull(message);
    assertEquals(message.getValueBytes(), valueBytes);
    assertEquals(message.getValueSchemaId(), 10);
    assertEquals(message.getRmdVersionId(), 5);
  }

  @Test
  public void testExtractNullValueWithRmdKept() throws Exception {
    // Null value but non-null RMD → record is kept (DELETE with RMD)
    TestablePartitionWriter writer = createConfiguredWriter(10, 5);

    byte[] keyBytes = "test-key".getBytes();
    byte[] rmdBytes = "test-rmd".getBytes();

    AbstractPartitionWriter.VeniceRecordWithMetadata record =
        new AbstractPartitionWriter.VeniceRecordWithMetadata(null, rmdBytes, 42, 7);
    Iterator<AbstractPartitionWriter.VeniceRecordWithMetadata> values = Collections.singletonList(record).iterator();

    AbstractPartitionWriter.VeniceWriterMessage message =
        writer.testExtract(keyBytes, values, mock(DataWriterTaskTracker.class));

    assertNotNull(message);
    assertNull(message.getValueBytes()); // Delete
    assertEquals(message.getValueSchemaId(), 42);
    assertEquals(message.getRmdVersionId(), 7);
  }

  /**
   * Minimal concrete subclass of AbstractPartitionWriter for testing extract().
   */
  private static class TestablePartitionWriter extends AbstractPartitionWriter {
    @Override
    protected void configureTask(VeniceProperties props) {
      // No-op for testing
    }

    AbstractPartitionWriter.VeniceWriterMessage testExtract(
        byte[] keyBytes,
        Iterator<AbstractPartitionWriter.VeniceRecordWithMetadata> values,
        DataWriterTaskTracker tracker) {
      return extract(keyBytes, values, tracker);
    }
  }

  private TestablePartitionWriter createConfiguredWriter(int globalValueSchemaId, int globalRmdSchemaId)
      throws Exception {
    TestablePartitionWriter writer = new TestablePartitionWriter();

    Properties props = new Properties();
    AbstractPartitionWriter.DuplicateKeyPrinter printer =
        new AbstractPartitionWriter.DuplicateKeyPrinter(new VeniceProperties(props));

    setField(writer, "duplicateKeyPrinter", printer);
    setField(writer, "valueSchemaId", globalValueSchemaId);
    setField(writer, "rmdSchemaId", globalRmdSchemaId);
    setField(writer, "callback", mock(PubSubProducerCallback.class));

    return writer;
  }

  private static void setField(Object target, String fieldName, Object value) throws Exception {
    Field field = AbstractPartitionWriter.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(target, value);
  }
}
