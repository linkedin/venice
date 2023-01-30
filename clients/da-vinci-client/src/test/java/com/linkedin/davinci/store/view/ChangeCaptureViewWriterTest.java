package com.linkedin.davinci.store.view;

import static com.linkedin.venice.schema.rmd.RmdConstants.*;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.davinci.config.VeniceConfigLoader;
import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.venice.client.change.capture.protocol.RecordChangeEvent;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.schema.rmd.RmdSchemaGenerator;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class ChangeCaptureViewWriterTest {
  private static final Schema SCHEMA = AvroCompatibilityHelper.parse("\"string\"");
  private static final byte[] KEY = "fishy_name".getBytes();
  private static final ByteBuffer OLD_VALUE = ByteBuffer.wrap("herring".getBytes());
  private static final ByteBuffer NEW_VALUE = ByteBuffer.wrap("silver_darling".getBytes());

  @Test
  public void testProcessRecord() {

    // Set up mocks
    Store mockStore = Mockito.mock(Store.class);
    VeniceProperties props = new VeniceProperties();
    Object2IntMap<String> urlMappingMap = new Object2IntOpenHashMap<>();
    Future<RecordMetadata> mockFuture = Mockito.mock(Future.class);

    VeniceWriter mockVeniceWriter = Mockito.mock(VeniceWriter.class);
    Mockito.when(mockVeniceWriter.put(Mockito.any(), Mockito.any(), Mockito.anyInt())).thenReturn(mockFuture);

    VeniceServerConfig mockVeniceServerConfig = Mockito.mock(VeniceServerConfig.class);
    Mockito.when(mockVeniceServerConfig.getKafkaClusterUrlToIdMap()).thenReturn(urlMappingMap);

    VeniceConfigLoader mockVeniceConfigLoader = Mockito.mock(VeniceConfigLoader.class);
    Mockito.when(mockVeniceConfigLoader.getCombinedProperties()).thenReturn(props);
    Mockito.when(mockVeniceConfigLoader.getVeniceServerConfig()).thenReturn(mockVeniceServerConfig);

    ChangeCaptureViewWriter changeCaptureViewWriter =
        new ChangeCaptureViewWriter(mockVeniceConfigLoader, mockStore, SCHEMA, Collections.emptyMap());

    Schema rmdSchema = RmdSchemaGenerator.generateMetadataSchema(SCHEMA, 1);
    List<Long> vectors = Arrays.asList(1L, 2L, 3L);
    GenericRecord rmdRecordWithValueLevelTimeStamp = new GenericData.Record(rmdSchema);
    rmdRecordWithValueLevelTimeStamp.put(TIMESTAMP_FIELD_NAME, 20L);
    rmdRecordWithValueLevelTimeStamp.put(REPLICATION_CHECKPOINT_VECTOR_FIELD, vectors);
    changeCaptureViewWriter.setVeniceWriter(mockVeniceWriter);

    changeCaptureViewWriter.processRecord(NEW_VALUE, OLD_VALUE, KEY, 1, 1, 1, rmdRecordWithValueLevelTimeStamp);

    // Set up argument captors
    ArgumentCaptor<byte[]> keyCaptor = ArgumentCaptor.forClass(byte[].class);
    ArgumentCaptor<RecordChangeEvent> eventCaptor = ArgumentCaptor.forClass(RecordChangeEvent.class);

    // Verify and capture input
    Mockito.verify(mockVeniceWriter).put(keyCaptor.capture(), eventCaptor.capture(), Mockito.eq(1));

    Assert.assertEquals(keyCaptor.getValue(), KEY);
    RecordChangeEvent recordChangeEvent = eventCaptor.getValue();
    Assert.assertEquals(recordChangeEvent.key.array(), KEY);
    Assert.assertEquals(recordChangeEvent.replicationCheckpointVector, vectors);
    Assert.assertEquals(recordChangeEvent.currentValue.value, NEW_VALUE);
    Assert.assertEquals(recordChangeEvent.previousValue.value, OLD_VALUE);
  }
}
