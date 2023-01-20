package com.linkedin.venice.router.api;

import static com.linkedin.venice.HttpConstants.VENICE_SCHEMA_ID;
import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_SCHEMA_ID;
import static com.linkedin.venice.system.store.MetaStoreWriter.KEY_STRING_STORE_NAME;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.read.RequestType;
import com.linkedin.venice.router.api.path.VenicePath;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.system.store.MetaStoreDataType;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestMetaStoreShadowReader {
  private final static String storeName = "testStore";
  private final RecordSerializer<StoreMetaKey> keySerializer =
      SerializerDeserializerFactory.getAvroGenericSerializer(StoreMetaKey.getClassSchema());
  private final RecordDeserializer<StoreMetaValue> valueDeserializer =
      SerializerDeserializerFactory.getAvroSpecificDeserializer(StoreMetaValue.class);

  @Test
  public void testShouldPerformShadowRead() {
    VenicePath mockVenicePath = mock(VenicePath.class);
    FullHttpResponse mockHttpResponse = mock(FullHttpResponse.class);
    ReadOnlySchemaRepository mockSchemaRepo = mock(ReadOnlySchemaRepository.class);
    MetaStoreShadowReader metaStoreShadowReader = new MetaStoreShadowReader(mockSchemaRepo);

    // In expected request setup we should perform shadow read.
    when(mockVenicePath.getStoreName()).thenReturn(VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName));
    when(mockVenicePath.isStreamingRequest()).thenReturn(false);
    when(mockVenicePath.getRequestType()).thenReturn(RequestType.SINGLE_GET);
    when(mockHttpResponse.status()).thenReturn(HttpResponseStatus.NOT_FOUND);
    Assert.assertTrue(metaStoreShadowReader.shouldPerformShadowRead(mockVenicePath, mockHttpResponse));

    // In other request setup we should not perform shadow read.
    when(mockVenicePath.getStoreName()).thenReturn(storeName);
    Assert.assertFalse(metaStoreShadowReader.shouldPerformShadowRead(mockVenicePath, mockHttpResponse));

    when(mockVenicePath.getStoreName()).thenReturn(VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName));
    when(mockVenicePath.isStreamingRequest()).thenReturn(true);
    Assert.assertFalse(metaStoreShadowReader.shouldPerformShadowRead(mockVenicePath, mockHttpResponse));

    when(mockVenicePath.isStreamingRequest()).thenReturn(false);
    when(mockVenicePath.getRequestType()).thenReturn(RequestType.MULTI_GET);
    Assert.assertFalse(metaStoreShadowReader.shouldPerformShadowRead(mockVenicePath, mockHttpResponse));

    when(mockVenicePath.getRequestType()).thenReturn(RequestType.SINGLE_GET);
    when(mockHttpResponse.status()).thenReturn(HttpResponseStatus.OK);
    Assert.assertFalse(metaStoreShadowReader.shouldPerformShadowRead(mockVenicePath, mockHttpResponse));
  }

  @Test
  public void testCreateStoreMetaValueAsResponse() {
    Schema keySchema = AvroCompatibilityHelper.parse("\"string\"");
    SchemaEntry keySchemaEntry = new SchemaEntry(1, keySchema);
    Schema valueSchema1 = AvroCompatibilityHelper.parse("\"string\"");
    SchemaEntry valueSchemaEntry1 = new SchemaEntry(1, valueSchema1);
    Schema valueSchema2 = AvroCompatibilityHelper.parse("\"int\"");
    SchemaEntry valueSchemaEntry2 = new SchemaEntry(2, valueSchema2);
    Collection<SchemaEntry> valueSchemaEntries = new ArrayList<>();
    valueSchemaEntries.add(valueSchemaEntry1);
    valueSchemaEntries.add(valueSchemaEntry2);

    ReadOnlySchemaRepository mockSchemaRepo = mock(ReadOnlySchemaRepository.class);
    when(mockSchemaRepo.getKeySchema(storeName)).thenReturn(keySchemaEntry);
    when(mockSchemaRepo.getValueSchema(storeName, 1)).thenReturn(valueSchemaEntry1);
    when(mockSchemaRepo.getValueSchema(storeName, 2)).thenReturn(valueSchemaEntry2);
    when(mockSchemaRepo.getValueSchemas(storeName)).thenReturn(valueSchemaEntries);
    MetaStoreShadowReader metaStoreShadowReader = new MetaStoreShadowReader(mockSchemaRepo);

    FullHttpResponse nullValueResponse = mock(FullHttpResponse.class);
    when(nullValueResponse.status()).thenReturn(HttpResponseStatus.NOT_FOUND);

    // Validate key schema request.
    StoreMetaKey key =
        MetaStoreDataType.STORE_KEY_SCHEMAS.getStoreMetaKey(Collections.singletonMap(KEY_STRING_STORE_NAME, storeName));
    VenicePath mockVenicePath = createMockVenicePath(key);
    FullHttpResponse updatedResponse = metaStoreShadowReader.shadowReadMetaStore(mockVenicePath, nullValueResponse);
    validateUpdatedResponseCommonProperties(updatedResponse);
    StoreMetaValue value = valueDeserializer.deserialize(updatedResponse.content().array());
    String keySchemaStr = value.storeKeySchemas.keySchemaMap.entrySet().iterator().next().getValue().toString();
    Assert.assertEquals(keySchemaStr, keySchema.toString());

    // Validate single value schema request.
    Map<String, String> keyParams = new HashMap<>();
    keyParams.put(KEY_STRING_STORE_NAME, storeName);
    keyParams.put(KEY_STRING_SCHEMA_ID, "2");
    key = MetaStoreDataType.STORE_VALUE_SCHEMA.getStoreMetaKey(keyParams);
    mockVenicePath = createMockVenicePath(key);
    updatedResponse = metaStoreShadowReader.shadowReadMetaStore(mockVenicePath, nullValueResponse);
    validateUpdatedResponseCommonProperties(updatedResponse);
    value = valueDeserializer.deserialize(updatedResponse.content().array());
    String valueSchemaStr = value.storeValueSchema.valueSchema.toString();
    Assert.assertEquals(valueSchemaStr, valueSchema2.toString());

    // Validate all value schemas request.
    key = MetaStoreDataType.STORE_VALUE_SCHEMAS
        .getStoreMetaKey(Collections.singletonMap(KEY_STRING_STORE_NAME, storeName));
    mockVenicePath = createMockVenicePath(key);
    updatedResponse = metaStoreShadowReader.shadowReadMetaStore(mockVenicePath, nullValueResponse);
    validateUpdatedResponseCommonProperties(updatedResponse);
    value = valueDeserializer.deserialize(updatedResponse.content().array());
    Assert.assertEquals(value.storeValueSchemas.valueSchemaMap.size(), 2);
    Map<Integer, String> valueSchemaMap = new HashMap<>();
    value.storeValueSchemas.valueSchemaMap
        .forEach((k, v) -> valueSchemaMap.put(Integer.parseInt(k.toString()), v.toString()));
    Assert.assertEquals(valueSchemaMap.get(1), valueSchema1.toString());
    Assert.assertEquals(valueSchemaMap.get(2), valueSchema2.toString());
  }

  private VenicePath createMockVenicePath(StoreMetaKey key) {
    ByteBuffer keyByteBuffer = ByteBuffer.wrap(keySerializer.serialize(key));
    RouterKey mockRouterKey = mock(RouterKey.class);
    VenicePath mockVenicePath = mock(VenicePath.class);
    when(mockRouterKey.getKeyBuffer()).thenReturn(keyByteBuffer);
    when(mockVenicePath.getStoreName()).thenReturn(VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName));
    when(mockVenicePath.getPartitionKey()).thenReturn(mockRouterKey);
    return mockVenicePath;
  }

  private void validateUpdatedResponseCommonProperties(FullHttpResponse updatedResponse) {
    Assert.assertEquals(updatedResponse.status(), HttpResponseStatus.OK);
    Assert.assertNotNull(updatedResponse.content());
    int writerSchemaId = updatedResponse.headers().getInt(VENICE_SCHEMA_ID);
    int expectedWriterSchemaId = AvroProtocolDefinition.METADATA_SYSTEM_SCHEMA_STORE.getCurrentProtocolVersion();
    Assert.assertEquals(writerSchemaId, expectedWriterSchemaId);
  }
}
