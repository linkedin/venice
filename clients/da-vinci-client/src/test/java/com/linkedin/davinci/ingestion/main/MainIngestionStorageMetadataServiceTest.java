package com.linkedin.davinci.ingestion.main;

import static com.linkedin.venice.serialization.avro.AvroProtocolDefinition.PARTITION_STATE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.stats.MetadataUpdateStats;
import com.linkedin.venice.offsets.OffsetRecord;
import com.linkedin.venice.serialization.avro.InternalAvroSpecificSerializer;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.function.BiConsumer;
import org.testng.annotations.Test;


public class MainIngestionStorageMetadataServiceTest {
  @Test
  public void testGetLastOffset() {
    VeniceConfigLoader configLoader = mock(VeniceConfigLoader.class);
    when(configLoader.getCombinedProperties()).thenReturn(VeniceProperties.empty());
    when(configLoader.getVeniceServerConfig()).thenReturn(mock(VeniceServerConfig.class));

    MainIngestionStorageMetadataService mainIngestionStorageMetadataService = new MainIngestionStorageMetadataService(
        0,
        mock(InternalAvroSpecificSerializer.class),
        mock(MetadataUpdateStats.class),
        configLoader,
        mock(BiConsumer.class));

    String topicName = "blah";
    int partition = 0;
    OffsetRecord offsetRecord1 = mainIngestionStorageMetadataService.getLastOffset(topicName, partition);

    assertNotNull(offsetRecord1);

    OffsetRecord offsetRecord2 = new OffsetRecord(PARTITION_STATE.getSerializer());
    offsetRecord2.setCheckpointLocalVersionTopicOffset(10);

    mainIngestionStorageMetadataService.putOffsetRecord(topicName, partition, offsetRecord2);
    OffsetRecord offsetRecord3 = mainIngestionStorageMetadataService.getLastOffset(topicName, partition);
    assertNotNull(offsetRecord3);
    assertEquals(offsetRecord3, offsetRecord2);
    assertNotEquals(
        offsetRecord3,
        offsetRecord1,
        "The offset record in the metadata service should now be different from the initial one");
  }
}
