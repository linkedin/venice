package com.linkedin.davinci.kafka.consumer;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.message.KafkaKey;
import com.linkedin.venice.pubsub.api.PubSubMessage;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.testng.annotations.Test;


public class LeaderProducerCallbackTest {
  @Test
  public void testOnCompletionWithNonNullException() {
    Logger loggerMock = mock(Logger.class);
    LeaderFollowerStoreIngestionTask ingestionTaskMock = mock(LeaderFollowerStoreIngestionTask.class);
    PubSubMessage<KafkaKey, KafkaMessageEnvelope, Long> sourceConsumerRecord = mock(PubSubMessage.class);
    PartitionConsumptionState partitionConsumptionState = mock(PartitionConsumptionState.class);
    LeaderProducedRecordContext leaderProducedRecordContext = mock(LeaderProducedRecordContext.class);
    int subPartition = 5;
    String kafkaUrl = "dc-0.kafka.venicedb.org";
    long beforeProcessingRecordTimestamp = 67454542;

    // ErrorCountAppender errorCountAppender

    LeaderProducerCallback leaderProducerCallback = new LeaderProducerCallback(
        ingestionTaskMock,
        sourceConsumerRecord,
        partitionConsumptionState,
        leaderProducedRecordContext,
        subPartition,
        kafkaUrl,
        beforeProcessingRecordTimestamp);

  }

  static class SimpleAppender extends AbstractAppender {
    List<String> logs = new ArrayList<>(100);

    protected SimpleAppender(
        String name,
        Filter filter,
        Layout<? extends Serializable> layout,
        boolean ignoreExceptions,
        Property[] properties) {
      super(name, filter, layout, ignoreExceptions, properties);
    }

    @Override
    public void append(LogEvent event) {
      logs.add(event.getMessage().getFormattedMessage());
    }
  }
}
