package com.linkedin.venice.schema;

import static com.linkedin.venice.utils.TestWriteUtils.loadFileAsStringQuietlyWithErrorLogged;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.exceptions.VeniceNoStoreException;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.schema.writecompute.DerivedSchemaEntry;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import java.util.Arrays;
import java.util.Collections;
import org.apache.avro.Schema;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class SchemaRepoBackedSchemaReaderTest {
  private static final Schema VALUE_SCHEMA_1 = AvroSchemaParseUtils
      .parseSchemaFromJSONLooseValidation(loadFileAsStringQuietlyWithErrorLogged("TestWriteComputeBuilder.avsc"));
  private static final Schema VALUE_SCHEMA_2 = AvroSchemaParseUtils.parseSchemaFromJSONLooseValidation(
      loadFileAsStringQuietlyWithErrorLogged("TestEvolvedWriteComputeBuilder.avsc"));
  private static final Schema UPDATE_SCHEMA_1 =
      WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(VALUE_SCHEMA_1);
  private static final Schema UPDATE_SCHEMA_2 =
      WriteComputeSchemaConverter.getInstance().convertFromValueRecordSchema(VALUE_SCHEMA_2);

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

  @Test
  public void testGetUpdateSchema() {
    String storeName = "test_store";
    String keySchemaStr = "\"string\"";
    SchemaEntry valueSchemaEntry1 = new SchemaEntry(1, VALUE_SCHEMA_1);
    SchemaEntry valueSchemaEntry2 = new SchemaEntry(2, VALUE_SCHEMA_2);
    DerivedSchemaEntry derivedSchemaEntry1 = new DerivedSchemaEntry(1, 1, UPDATE_SCHEMA_1);
    DerivedSchemaEntry derivedSchemaEntry2 = new DerivedSchemaEntry(2, 1, UPDATE_SCHEMA_2);
    // setup key schema
    SchemaEntry keySchemaEntry = new SchemaEntry(1, keySchemaStr);

    ReadOnlySchemaRepository mockSchemaRepository = Mockito.mock(ReadOnlySchemaRepository.class);
    Mockito.doReturn(keySchemaEntry).when(mockSchemaRepository).getKeySchema(storeName);
    Mockito.doReturn(valueSchemaEntry1).when(mockSchemaRepository).getValueSchema(storeName, 1);
    Mockito.doReturn(valueSchemaEntry2).when(mockSchemaRepository).getValueSchema(storeName, 2);
    Mockito.doReturn(valueSchemaEntry2).when(mockSchemaRepository).getSupersetOrLatestValueSchema(storeName);
    Mockito.doReturn(Arrays.asList(derivedSchemaEntry1, derivedSchemaEntry2))
        .when(mockSchemaRepository)
        .getDerivedSchemas(storeName);

    SchemaReader schemaReader = new SchemaRepoBackedSchemaReader(mockSchemaRepository, storeName);

    Assert.assertEquals(schemaReader.getUpdateSchema(1).toString(), UPDATE_SCHEMA_1.toString());
    Assert.assertEquals(schemaReader.getUpdateSchema(2).toString(), UPDATE_SCHEMA_2.toString());

    DerivedSchemaEntry latestUpdateSchema = schemaReader.getLatestUpdateSchema();
    Assert.assertEquals(latestUpdateSchema.getSchema().toString(), UPDATE_SCHEMA_2.toString());
    Assert.assertEquals(latestUpdateSchema.getValueSchemaID(), 2);
    Assert.assertEquals(latestUpdateSchema.getId(), 1);
  }

  @Test
  public void testGetUpdateSchemaWhenNoValueSchema() throws VeniceNoStoreException {
    String storeName = "test_store";
    String keySchemaStr = "\"string\"";

    SchemaEntry valueSchemaEntry1 = new SchemaEntry(1, VALUE_SCHEMA_1);
    DerivedSchemaEntry derivedSchemaEntry1 = new DerivedSchemaEntry(1, 1, UPDATE_SCHEMA_1);
    // setup key schema
    SchemaEntry keySchemaEntry = new SchemaEntry(1, keySchemaStr);

    ReadOnlySchemaRepository mockSchemaRepository = Mockito.mock(ReadOnlySchemaRepository.class);
    Mockito.doReturn(keySchemaEntry).when(mockSchemaRepository).getKeySchema(storeName);
    Mockito.doReturn(valueSchemaEntry1).when(mockSchemaRepository).getValueSchema(storeName, 1);
    Mockito.doReturn(null).when(mockSchemaRepository).getValueSchema(storeName, 2);
    Mockito.doReturn(valueSchemaEntry1).when(mockSchemaRepository).getSupersetOrLatestValueSchema(storeName);
    Mockito.doReturn(Collections.singletonList(derivedSchemaEntry1))
        .when(mockSchemaRepository)
        .getDerivedSchemas(storeName);

    SchemaReader schemaReader = new SchemaRepoBackedSchemaReader(mockSchemaRepository, storeName);

    Schema schema = schemaReader.getUpdateSchema(1);
    Assert.assertEquals(schema.toString(), UPDATE_SCHEMA_1.toString());
    Assert.assertNull(schemaReader.getUpdateSchema(2));
    Assert.assertEquals(schemaReader.getLatestUpdateSchema().getSchemaStr(), UPDATE_SCHEMA_1.toString());
  }

  @Test
  public void testGetUpdateSchemaWhenNoUpdateSchema() throws VeniceNoStoreException {
    String storeName = "test_store";
    String keySchemaStr = "\"string\"";

    SchemaEntry valueSchemaEntry1 = new SchemaEntry(1, VALUE_SCHEMA_1);
    // setup key schema
    SchemaEntry keySchemaEntry = new SchemaEntry(1, keySchemaStr);

    ReadOnlySchemaRepository mockSchemaRepository = Mockito.mock(ReadOnlySchemaRepository.class);
    Mockito.doReturn(keySchemaEntry).when(mockSchemaRepository).getKeySchema(storeName);
    Mockito.doReturn(valueSchemaEntry1).when(mockSchemaRepository).getValueSchema(storeName, 1);
    Mockito.doReturn(null).when(mockSchemaRepository).getValueSchema(storeName, 2);
    Mockito.doReturn(valueSchemaEntry1).when(mockSchemaRepository).getSupersetOrLatestValueSchema(storeName);
    Mockito.doReturn(Collections.emptyList()).when(mockSchemaRepository).getDerivedSchemas(storeName);

    SchemaReader schemaReader = new SchemaRepoBackedSchemaReader(mockSchemaRepository, storeName);

    Assert.assertThrows(VeniceException.class, () -> schemaReader.getUpdateSchema(1));
    Assert.assertThrows(VeniceException.class, () -> schemaReader.getLatestUpdateSchema());
  }
}
