package com.linkedin.venice.schema;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import org.apache.avro.Schema;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SchemaRepoBackedSchemaReaderTest {
  @Test
  public void testGetKeySchema() {
    String storeName = "test_store";
    String keySchemaStr = "\"string\"";
    ReadOnlySchemaRepository mockSchemaRepository = Mockito.mock(ReadOnlySchemaRepository.class);
    SchemaEntry schemaEntry = new SchemaEntry(1, keySchemaStr);
    Mockito.doReturn(schemaEntry).when(mockSchemaRepository).getKeySchema(storeName);

    SchemaReader schemaReader = new SchemaRepoBackedSchemaReader(mockSchemaRepository, storeName);
    Schema schema = schemaReader.getKeySchema();
    Assert.assertEquals(schema.toString(), keySchemaStr);
  }

  @Test(expectedExceptions = VeniceNoStoreException.class)
  public void testGetKeySchemaWhenNotExists() throws VeniceNoStoreException {
    String storeName = "test_store";
    ReadOnlySchemaRepository mockSchemaRepository = Mockito.mock(ReadOnlySchemaRepository.class);
    Mockito.doThrow(new VeniceNoStoreException(storeName)).when(mockSchemaRepository).getKeySchema(storeName);

    SchemaReader schemaReader = new SchemaRepoBackedSchemaReader(mockSchemaRepository, storeName);
    schemaReader.getKeySchema();
  }

  @Test
  public void testGetValueSchema() {
    String storeName = "test_store";
    String keySchemaStr = "\"string\"";
    int valueSchemaId1 = 1;
    String valueSchemaStr1 = "\"string\"";
    int valueSchemaId2 = 2;
    String valueSchemaStr2 = "\"long\"";
    SchemaEntry schemaEntry1 = new SchemaEntry(valueSchemaId1, valueSchemaStr1);
    SchemaEntry schemaEntry2 = new SchemaEntry(valueSchemaId2, valueSchemaStr2);
    // setup key schema
    SchemaEntry keySchemaEntry = new SchemaEntry(1, keySchemaStr);

    ReadOnlySchemaRepository mockSchemaRepository = Mockito.mock(ReadOnlySchemaRepository.class);
    Mockito.doReturn(keySchemaEntry).when(mockSchemaRepository).getKeySchema(storeName);
    Mockito.doReturn(schemaEntry1).when(mockSchemaRepository).getValueSchema(storeName, 1);
    Mockito.doReturn(schemaEntry2).when(mockSchemaRepository).getValueSchema(storeName, 2);
    Mockito.doReturn(schemaEntry2).when(mockSchemaRepository).getSupersetOrLatestValueSchema(storeName);

    SchemaReader schemaReader = new SchemaRepoBackedSchemaReader(mockSchemaRepository, storeName);

    int valueSchemaId = 1;
    String valueSchemaStr = "\"string\"";

    Schema schema = schemaReader.getValueSchema(valueSchemaId);
    Assert.assertEquals(schema.toString(), valueSchemaStr);
    Assert.assertEquals(schemaReader.getLatestValueSchema().toString(), valueSchemaStr2);
  }

  @Test
  public void testGetValueSchemaWhenNotExists() throws VeniceNoStoreException {
    String storeName = "test_store";
    String invalidStoreName = "invalid_store";
    String keySchemaStr = "\"string\"";
    int valueSchemaId1 = 1;
    String valueSchemaStr1 = "\"string\"";
    SchemaEntry schemaEntry1 = new SchemaEntry(valueSchemaId1, valueSchemaStr1);
    // setup key schema
    SchemaEntry keySchemaEntry = new SchemaEntry(1, keySchemaStr);

    ReadOnlySchemaRepository mockSchemaRepository = Mockito.mock(ReadOnlySchemaRepository.class);
    Mockito.doReturn(keySchemaEntry).when(mockSchemaRepository).getKeySchema(storeName);
    Mockito.doReturn(schemaEntry1).when(mockSchemaRepository).getValueSchema(storeName, 1);
    Mockito.doReturn(null).when(mockSchemaRepository).getValueSchema(storeName, 2);
    Mockito.doReturn(schemaEntry1).when(mockSchemaRepository).getSupersetOrLatestValueSchema(storeName);

    Mockito.doThrow(new VeniceNoStoreException(invalidStoreName))
        .when(mockSchemaRepository)
        .getValueSchema(invalidStoreName, 1);
    SchemaReader schemaReader = new SchemaRepoBackedSchemaReader(mockSchemaRepository, storeName);
    SchemaReader invalidStoreSchemaReader = new SchemaRepoBackedSchemaReader(mockSchemaRepository, invalidStoreName);

    Assert.assertThrows(VeniceNoStoreException.class, () -> invalidStoreSchemaReader.getValueSchema(1));

    Schema schema = schemaReader.getValueSchema(1);
    Assert.assertEquals(schema.toString(), valueSchemaStr1);
    Assert.assertNull(schemaReader.getValueSchema(2));
    Assert.assertEquals(schemaReader.getLatestValueSchema().toString(), valueSchemaStr1);
  }

  @Test
  public void testGetLatestValueSchema() {
    String storeName = "test_store";
    String keySchemaStr = "\"string\"";
    int valueSchemaId1 = 1;
    String valueSchemaStr1 = "\"string\"";
    int valueSchemaId2 = 2;
    String valueSchemaStr2 = "\"long\"";
    SchemaEntry schemaEntry1 = new SchemaEntry(valueSchemaId1, valueSchemaStr1);
    SchemaEntry schemaEntry2 = new SchemaEntry(valueSchemaId2, valueSchemaStr2);
    // setup key schema
    SchemaEntry keySchemaEntry = new SchemaEntry(1, keySchemaStr);

    ReadOnlySchemaRepository mockSchemaRepository = Mockito.mock(ReadOnlySchemaRepository.class);
    Mockito.doReturn(keySchemaEntry).when(mockSchemaRepository).getKeySchema(storeName);
    Mockito.doReturn(schemaEntry1).when(mockSchemaRepository).getValueSchema(storeName, 1);
    Mockito.doReturn(schemaEntry2).when(mockSchemaRepository).getValueSchema(storeName, 2);
    Mockito.doReturn(schemaEntry2).when(mockSchemaRepository).getSupersetOrLatestValueSchema(storeName);

    SchemaReader schemaReader = new SchemaRepoBackedSchemaReader(mockSchemaRepository, storeName);

    Assert.assertEquals(schemaReader.getLatestValueSchema().toString(), valueSchemaStr2);
    Assert.assertEquals(schemaReader.getValueSchema(valueSchemaId1).toString(), valueSchemaStr1);
    Assert.assertEquals(schemaReader.getValueSchema(valueSchemaId2).toString(), valueSchemaStr2);
  }

  @Test(expectedExceptions = VeniceException.class)
  public void testGetLatestValueSchemaWhenNoValueSchema() {
    String storeName = "test_store";

    ReadOnlySchemaRepository mockSchemaRepository = Mockito.mock(ReadOnlySchemaRepository.class);
    Mockito.doThrow(new VeniceException(storeName + " doesn't have latest schema!"))
        .when(mockSchemaRepository)
        .getSupersetOrLatestValueSchema(storeName);

    SchemaReader schemaReader = new SchemaRepoBackedSchemaReader(mockSchemaRepository, storeName);
    schemaReader.getLatestValueSchema();
  }
}
