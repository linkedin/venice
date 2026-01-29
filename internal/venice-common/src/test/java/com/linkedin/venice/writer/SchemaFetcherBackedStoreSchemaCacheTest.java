package com.linkedin.venice.writer;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.client.schema.StoreSchemaFetcher;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SchemaFetcherBackedStoreSchemaCacheTest {
  private static final Schema VALUE_SCHEMA = AvroCompatibilityHelper.parse(TestUtils.loadFileAsString("PersonV1.avsc"));
  private static final Schema EVOLVED_VALUE_SCHEMA =
      AvroCompatibilityHelper.parse(TestUtils.loadFileAsString("PersonV2.avsc"));
  private static final Schema UPDATE_SCHEMA =
      WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(VALUE_SCHEMA);
  private static final Schema EVOLVED_UPDATE_SCHEMA =
      WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(EVOLVED_VALUE_SCHEMA);

  @Test()
  public void testSchemaRetrival() {
    StoreSchemaFetcher schemaFetcher = mock(StoreSchemaFetcher.class);
    Map<Integer, Schema> schemaMap = new VeniceConcurrentHashMap<>();
    schemaMap.put(1, VALUE_SCHEMA);
    doReturn(schemaMap).when(schemaFetcher).getAllValueSchemasWithId();
    doReturn(EVOLVED_UPDATE_SCHEMA).when(schemaFetcher).getUpdateSchema(EVOLVED_VALUE_SCHEMA);
    doReturn(UPDATE_SCHEMA).when(schemaFetcher).getUpdateSchema(VALUE_SCHEMA);

    SchemaEntry schemaEntryV1 = new SchemaEntry(1, VALUE_SCHEMA);
    SchemaEntry updateSchemaEntryV1 = new DerivedSchemaEntry(1, 1, UPDATE_SCHEMA);
    doReturn(schemaEntryV1).when(schemaFetcher).getLatestValueSchemaEntry();
    doReturn(updateSchemaEntryV1).when(schemaFetcher).getUpdateSchemaEntry(1);

    SchemaFetcherBackedStoreSchemaCache storeSchemaCache = new SchemaFetcherBackedStoreSchemaCache(schemaFetcher);
    Assert.assertEquals(storeSchemaCache.getUpdateSchema(), UPDATE_SCHEMA);
    Assert.assertEquals(storeSchemaCache.getSupersetSchema(), VALUE_SCHEMA);
    Assert.assertEquals(storeSchemaCache.getLatestOrSupersetSchemaId(), 1);
    Assert.assertEquals(storeSchemaCache.getValueSchemaMap().size(), 1);

    schemaMap.put(2, EVOLVED_VALUE_SCHEMA);
    SchemaEntry schemaEntryV2 = new SchemaEntry(2, EVOLVED_VALUE_SCHEMA);
    SchemaEntry updateSchemaEntryV2 = new DerivedSchemaEntry(2, 1, EVOLVED_UPDATE_SCHEMA);
    doReturn(schemaEntryV2).when(schemaFetcher).getLatestValueSchemaEntry();
    doReturn(updateSchemaEntryV2).when(schemaFetcher).getUpdateSchemaEntry(2);
    doReturn(schemaEntryV2).when(schemaFetcher).getLatestValueSchemaEntry();

    // Store Cache does not automatically refresh itself unless notified.
    Assert.assertEquals(storeSchemaCache.getUpdateSchema(), UPDATE_SCHEMA);
    Assert.assertEquals(storeSchemaCache.getSupersetSchema(), VALUE_SCHEMA);
    Assert.assertEquals(storeSchemaCache.getLatestOrSupersetSchemaId(), 1);
    Assert.assertEquals(storeSchemaCache.getValueSchemaMap().size(), 1);

    storeSchemaCache.maybeUpdateSupersetSchema(2);

    // Store Cache should use new information now.
    Assert.assertEquals(storeSchemaCache.getUpdateSchema(), EVOLVED_UPDATE_SCHEMA);
    Assert.assertEquals(storeSchemaCache.getSupersetSchema(), EVOLVED_VALUE_SCHEMA);
    Assert.assertEquals(storeSchemaCache.getLatestOrSupersetSchemaId(), 2);
    Assert.assertEquals(storeSchemaCache.getValueSchemaMap().size(), 2);
  }
}
