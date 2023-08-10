package com.linkedin.davinci.kafka.consumer;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertNotNull;

import com.linkedin.davinci.schema.merge.MergeRecordHelper;
import com.linkedin.venice.client.store.schemas.TestValueRecord;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.utils.DataProviderUtils;
import com.linkedin.venice.utils.TestUtils;
import org.apache.avro.Schema;
import org.testng.annotations.Test;


public class TestStoreWriteComputeProcessor {
  @Test(dataProviderClass = DataProviderUtils.class, dataProvider = "True-and-False")
  public void testFastAvroSerde(boolean fastAvroEnabled) {
    String storeName = TestUtils.getUniqueTopicString("test");
    ReadOnlySchemaRepository mockRepo = mock(ReadOnlySchemaRepository.class);
    MergeRecordHelper mockHelper = mock(MergeRecordHelper.class);
    Schema testSchema = TestValueRecord.SCHEMA$;
    doReturn(new SchemaEntry(1, testSchema)).when(mockRepo).getValueSchema(storeName, 1);

    StoreWriteComputeProcessor writeComputeProcessor =
        new StoreWriteComputeProcessor(storeName, mockRepo, mockHelper, fastAvroEnabled);
    assertNotNull(writeComputeProcessor.getValueDeserializer(testSchema, testSchema));
    assertNotNull(writeComputeProcessor.generateValueSerializer(1));
  }
}
