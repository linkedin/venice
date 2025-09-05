package com.linkedin.venice.hadoop.task.datawriter;

import static org.mockito.Mockito.mock;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.pubsub.api.PubSubProducerCallback;
import com.linkedin.venice.writer.VeniceWriter;
import java.nio.ByteBuffer;
import org.testng.annotations.Test;


public class AbstractPartitionWriterTest {
  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Found replication metadata without a valid schema id")
  public void testInvalidRmdSchemaWithValidRmdPayload() {
    final byte[] keys = new byte[] { 0, 1, 2 };
    final byte[] value = new byte[] { 0, 1, 2 };
    final byte[] rmd = new byte[] { 0, 1, 2 };
    AbstractPartitionWriter.VeniceWriterMessage message = new AbstractPartitionWriter.VeniceWriterMessage(
        keys,
        value,
        1,
        -1,
        ByteBuffer.wrap(rmd),
        mock(PubSubProducerCallback.class),
        false,
        1);
    message.getConsumer().accept(mock(VeniceWriter.class));
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Found empty replication metadata")
  public void testInvalidRmdSchemaWithInvalidRmdPayload() {
    final byte[] keys = new byte[] { 0, 1, 2 };
    final byte[] value = new byte[] { 0, 1, 2 };
    final byte[] rmd = new byte[0];
    AbstractPartitionWriter.VeniceWriterMessage message = new AbstractPartitionWriter.VeniceWriterMessage(
        keys,
        value,
        1,
        -1,
        ByteBuffer.wrap(rmd),
        mock(PubSubProducerCallback.class),
        false,
        1);
    message.getConsumer().accept(mock(VeniceWriter.class));
  }
}
