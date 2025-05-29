package com.linkedin.davinci.kafka.consumer;

import static org.mockito.Mockito.mock;

import com.linkedin.venice.exceptions.VeniceMessageException;
import com.linkedin.venice.kafka.protocol.Put;
import com.linkedin.venice.pubsub.api.PubSubPosition;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ProducedRecordTest {
  @Test
  public void testProducedRecordCompletionSuccess() throws Exception {
    byte[] key1 = new byte[] { 97, 98, 99 };
    byte[] value1 = new byte[] { 97, 98, 100 };

    Put put1 = new Put();
    put1.putValue = ByteBuffer.wrap(value1);
    put1.schemaId = 5;

    PubSubPosition consumedPositionMock = mock(PubSubPosition.class);
    LeaderProducedRecordContext pr1 = LeaderProducedRecordContext.newPutRecord(-1, consumedPositionMock, key1, put1);
    CompletableFuture.runAsync(() -> pr1.completePersistedToDBFuture(null));
    pr1.getPersistedToDBFuture().get(10, TimeUnit.SECONDS);
  }

  @Test
  public void testProducedRecordCompletionException() {
    byte[] key1 = new byte[] { 97, 98, 99 };
    byte[] value1 = new byte[] { 97, 98, 100 };

    Put put1 = new Put();
    put1.putValue = ByteBuffer.wrap(value1);
    put1.schemaId = 5;

    PubSubPosition consumedPositionMock = mock(PubSubPosition.class);
    LeaderProducedRecordContext pr1 = LeaderProducedRecordContext.newPutRecord(-1, consumedPositionMock, key1, put1);
    CompletableFuture.runAsync(() -> pr1.completePersistedToDBFuture(new VeniceMessageException("test exception")));

    try {
      pr1.getPersistedToDBFuture().get(10, TimeUnit.SECONDS);
    } catch (Exception e) {
      Assert.assertTrue(e.getCause() instanceof VeniceMessageException);
      Assert.assertTrue(e.getCause().getMessage().equalsIgnoreCase("test exception"));
    }
  }
}
