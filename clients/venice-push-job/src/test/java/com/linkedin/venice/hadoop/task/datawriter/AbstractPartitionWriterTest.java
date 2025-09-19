package com.linkedin.venice.hadoop.task.datawriter;

import static org.mockito.Mockito.mock;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.writer.VeniceWriter;
import java.nio.ByteBuffer;
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
}
