package com.linkedin.davinci.storage;

import static org.mockito.Mockito.mock;

import com.linkedin.davinci.kafka.consumer.KafkaStoreIngestionService;
import com.linkedin.davinci.stats.ingestion.heartbeat.HeartbeatMonitoringService;
import com.linkedin.venice.utils.ComplementSet;
import org.mockito.Mockito;
import org.testng.annotations.Test;


public class IngestionMetadataRetrieverDelegatorTest {
  @Test
  public void testInterface() {
    KafkaStoreIngestionService kafkaStoreIngestionService = mock(KafkaStoreIngestionService.class);
    HeartbeatMonitoringService heartbeatMonitoringService = mock(HeartbeatMonitoringService.class);
    IngestionMetadataRetrieverDelegator ingestionMetadataRetrieverDelegator =
        new IngestionMetadataRetrieverDelegator(kafkaStoreIngestionService, heartbeatMonitoringService);
    ingestionMetadataRetrieverDelegator.getHeartbeatLag("", -1, false);
    Mockito.verify(heartbeatMonitoringService, Mockito.times(1)).getHeartbeatInfo("", -1, false);
    ingestionMetadataRetrieverDelegator.getStoreVersionCompressionDictionary("dummy_v1");
    Mockito.verify(kafkaStoreIngestionService, Mockito.times(1)).getStoreVersionCompressionDictionary("dummy_v1");
    ingestionMetadataRetrieverDelegator.getTopicPartitionIngestionContext("dummy_v1", "dummy_rt", 1);
    Mockito.verify(kafkaStoreIngestionService, Mockito.times(1))
        .getTopicPartitionIngestionContext("dummy_v1", "dummy_rt", 1);
    ComplementSet<Integer> integerComplementSet = ComplementSet.of(1);
    ingestionMetadataRetrieverDelegator.getConsumptionSnapshots("dummy_v1", integerComplementSet);
    Mockito.verify(kafkaStoreIngestionService, Mockito.times(1))
        .getConsumptionSnapshots("dummy_v1", integerComplementSet);
    ;
  }
}
