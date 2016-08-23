package com.linkedin.venice.client.schema;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.exceptions.VeniceServerException;
import com.linkedin.venice.client.store.AbstractAvroStoreClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import org.apache.avro.Schema;
import org.codehaus.jackson.map.ObjectMapper;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class SchemaReaderTest {
  private ObjectMapper mapper = new ObjectMapper();
  private final int TIMEOUT = 3;

  @Test
  public void testGetKeySchema() throws IOException, ExecutionException, InterruptedException, VeniceClientException {
    String storeName = "test_store";
    String keySchemaStr = "\"string\"";
    AbstractAvroStoreClient mockClient = Mockito.mock(AbstractAvroStoreClient.class);
    Mockito.doReturn(storeName).when(mockClient).getStoreName();
    SchemaResponse schemaResponse = new SchemaResponse();
    schemaResponse.setId(1);
    schemaResponse.setSchemaStr(keySchemaStr);
    Future<byte[]> mockFuture = Mockito.mock(Future.class);
    Mockito.doReturn(mapper.writeValueAsBytes(schemaResponse)).when(mockFuture).get();
    Mockito.doReturn(mockFuture).when(mockClient).getRaw("key_schema/" + storeName);

    SchemaReader schemaReader = new SchemaReader(mockClient);
    Schema schema = schemaReader.getKeySchema();
    Assert.assertEquals(schema.toString(), keySchemaStr);
    Schema cachedSchema = schemaReader.getKeySchema();
    Assert.assertEquals(cachedSchema, schema);
    // Must be the same Schema instance
    Assert.assertTrue(schema == cachedSchema);
    Mockito.verify(mockClient, Mockito.timeout(TIMEOUT).times(1)).getRaw(Mockito.anyString());
  }

  @Test (expectedExceptions = VeniceClientException.class)
  public void testGetKeySchemaWhenNotExists() throws IOException, ExecutionException, InterruptedException, VeniceClientException {
    String storeName = "test_store";
    AbstractAvroStoreClient mockClient = Mockito.mock(AbstractAvroStoreClient.class);
    Mockito.doReturn(storeName).when(mockClient).getStoreName();
    Future<byte[]> mockFuture = Mockito.mock(Future.class);
    Mockito.doReturn(null).when(mockFuture).get();
    Mockito.doReturn(mockFuture).when(mockClient).getRaw("key_schema/" + storeName);
    new SchemaReader(mockClient);
  }

  @Test (expectedExceptions = VeniceClientException.class)
  public void testGetKeySchemaWhenServerError() throws ExecutionException, InterruptedException, VeniceClientException {
    String storeName = "test_store";
    AbstractAvroStoreClient mockClient = Mockito.mock(AbstractAvroStoreClient.class);
    Mockito.doReturn(storeName).when(mockClient).getStoreName();
    Future<byte[]> mockFuture = Mockito.mock(Future.class);
    Mockito.doThrow(new ExecutionException(new VeniceServerException("Server error"))).when(mockFuture).get();
    Mockito.doReturn(mockFuture).when(mockClient).getRaw("key_schema/" + storeName);
    new SchemaReader(mockClient);
  }

  @Test
  public void testGetValueSchema() throws IOException, ExecutionException, InterruptedException, VeniceClientException {
    String storeName = "test_store";
    String keySchemaStr = "\"string\"";
    String valueSchemaStr = "\"string\"";
    int valueSchemaId = 1;
    AbstractAvroStoreClient mockClient = Mockito.mock(AbstractAvroStoreClient.class);
    Mockito.doReturn(storeName).when(mockClient).getStoreName();
    // setup key schema
    SchemaResponse schemaResponse = new SchemaResponse();
    schemaResponse.setId(1);
    schemaResponse.setSchemaStr(keySchemaStr);
    Future<byte[]> mockFuture = Mockito.mock(Future.class);
    Mockito.doReturn(mapper.writeValueAsBytes(schemaResponse)).when(mockFuture).get();
    Mockito.doReturn(mockFuture).when(mockClient).getRaw("key_schema/" + storeName);
    schemaResponse = new SchemaResponse();
    schemaResponse.setId(valueSchemaId);
    schemaResponse.setSchemaStr(valueSchemaStr);
    Mockito.doReturn(mapper.writeValueAsBytes(schemaResponse)).when(mockFuture).get();
    Mockito.doReturn(mockFuture).when(mockClient).getRaw("value_schema/" + storeName + "/" + valueSchemaId);

    SchemaReader schemaReader = new SchemaReader(mockClient);
    Schema schema = schemaReader.getValueSchema(valueSchemaId);
    Assert.assertEquals(schema.toString(), valueSchemaStr);
    Schema cachedSchema = schemaReader.getValueSchema(valueSchemaId);
    Assert.assertEquals(cachedSchema, schema);
    // Must be the same Schema instance
    Assert.assertTrue(schema == cachedSchema);
    Assert.assertEquals(schemaReader.getLatestValueSchema().toString(), valueSchemaStr);
    Mockito.verify(mockClient, Mockito.timeout(TIMEOUT).times(2)).getRaw(Mockito.anyString());
  }

  @Test
  public void testGetValueSchemaWhenNotExists() throws IOException, ExecutionException, InterruptedException, VeniceClientException {
    String storeName = "test_store";
    String keySchemaStr = "\"string\"";
    String valueSchemaStr = "\"string\"";
    int valueSchemaId = 1;
    AbstractAvroStoreClient mockClient = Mockito.mock(AbstractAvroStoreClient.class);
    Mockito.doReturn(storeName).when(mockClient).getStoreName();
    // setup key schema
    SchemaResponse schemaResponse = new SchemaResponse();
    schemaResponse.setId(1);
    schemaResponse.setSchemaStr(keySchemaStr);
    Future<byte[]> mockFuture = Mockito.mock(Future.class);
    Mockito.doReturn(mapper.writeValueAsBytes(schemaResponse)).when(mockFuture).get();
    Mockito.doReturn(mockFuture).when(mockClient).getRaw("key_schema/" + storeName);

    SchemaReader schemaReader = new SchemaReader(mockClient);
    Mockito.doReturn(null).when(mockFuture).get();
    Mockito.doReturn(mockFuture).when(mockClient).getRaw("value_schema/" + storeName + "/" + valueSchemaId);
    Schema schema = schemaReader.getValueSchema(valueSchemaId);
    Assert.assertNull(schema);
    Schema cachedSchema = schemaReader.getValueSchema(valueSchemaId);
    Assert.assertNull(cachedSchema);
    Mockito.verify(mockClient, Mockito.timeout(TIMEOUT).times(3)).getRaw(Mockito.anyString());

    schemaResponse = new SchemaResponse();
    schemaResponse.setId(valueSchemaId);
    schemaResponse.setSchemaStr(valueSchemaStr);
    Mockito.doReturn(mapper.writeValueAsBytes(schemaResponse)).when(mockFuture).get();

    Schema newSchema = schemaReader.getValueSchema(valueSchemaId);
    Assert.assertEquals(newSchema.toString(), valueSchemaStr);
    Assert.assertEquals(schemaReader.getLatestValueSchema().toString(), valueSchemaStr);
  }

  @Test (expectedExceptions = RuntimeException.class)
  public void testGetValueSchemaWhenServerError() throws ExecutionException, InterruptedException, VeniceClientException, IOException {
    String storeName = "test_store";
    String keySchemaStr = "\"string\"";
    int valueSchemaId = 1;
    AbstractAvroStoreClient mockClient = Mockito.mock(AbstractAvroStoreClient.class);
    Mockito.doReturn(storeName).when(mockClient).getStoreName();
    // setup key schema
    SchemaResponse schemaResponse = new SchemaResponse();
    schemaResponse.setId(1);
    schemaResponse.setSchemaStr(keySchemaStr);
    Future<byte[]> mockFuture = Mockito.mock(Future.class);
    Mockito.doReturn(mapper.writeValueAsBytes(schemaResponse)).when(mockFuture).get();
    Mockito.doReturn(mockFuture).when(mockClient).getRaw("key_schema/" + storeName);
    SchemaReader schemaReader = new SchemaReader(mockClient);
    Mockito.doThrow(new ExecutionException(new VeniceServerException("Server error"))).when(mockFuture).get();
    Mockito.doReturn(mockFuture).when(mockClient).getRaw("value_schema/" + storeName + "/" + valueSchemaId);
    schemaReader.getValueSchema(valueSchemaId);

  }

  @Test
  public void testGetLatestValueSchema() throws IOException, ExecutionException, InterruptedException, VeniceClientException {
    String storeName = "test_store";
    String keySchemaStr = "\"string\"";
    int valueSchemaId1 = 1;
    String valueSchemaStr1 = "\"string\"";
    int valueSchemaId2 = 2;
    String valueSchemaStr2 = "\"long\"";
    MultiSchemaResponse multiSchemaResponse = new MultiSchemaResponse();
    MultiSchemaResponse.Schema[] schemas = new MultiSchemaResponse.Schema[2];
    MultiSchemaResponse.Schema schema1 = new MultiSchemaResponse.Schema();
    schema1.setId(valueSchemaId1);
    schema1.setSchemaStr(valueSchemaStr1);
    MultiSchemaResponse.Schema schema2 = new MultiSchemaResponse.Schema();
    schema2.setId(valueSchemaId2);
    schema2.setSchemaStr(valueSchemaStr2);
    schemas[0] = schema1;
    schemas[1] = schema2;
    multiSchemaResponse.setSchemas(schemas);
    // setup key schema
    SchemaResponse schemaResponse = new SchemaResponse();
    schemaResponse.setId(1);
    schemaResponse.setSchemaStr(keySchemaStr);
    AbstractAvroStoreClient mockClient = Mockito.mock(AbstractAvroStoreClient.class);
    Mockito.doReturn(storeName).when(mockClient).getStoreName();
    Future<byte[]> mockFuture = Mockito.mock(Future.class);
    Mockito.doReturn(mapper.writeValueAsBytes(schemaResponse)).when(mockFuture).get();
    Mockito.doReturn(mockFuture).when(mockClient).getRaw("key_schema/" + storeName);
    SchemaReader schemaReader = new SchemaReader(mockClient);

    Mockito.doReturn(mapper.writeValueAsBytes(multiSchemaResponse)).when(mockFuture).get();
    Mockito.doReturn(mockFuture).when(mockClient).getRaw("value_schema/" + storeName);
    Assert.assertEquals(schemaReader.getLatestValueSchema().toString(), valueSchemaStr2);
    Assert.assertEquals(schemaReader.getValueSchema(valueSchemaId1).toString(), valueSchemaStr1);
    Assert.assertEquals(schemaReader.getValueSchema(valueSchemaId2).toString(), valueSchemaStr2);
    schemaReader.getLatestValueSchema();
    Mockito.verify(mockClient, Mockito.timeout(TIMEOUT).times(2)).getRaw(Mockito.anyString());
  }

  @Test
  public void testGetLatestValueSchemaWhenNoValueSchema() throws IOException, ExecutionException, InterruptedException, VeniceClientException {
    String storeName = "test_store";
    String keySchemaStr = "\"string\"";
    MultiSchemaResponse multiSchemaResponse = new MultiSchemaResponse();
    multiSchemaResponse.setSchemas(new MultiSchemaResponse.Schema[0]);

    // setup key schema
    SchemaResponse schemaResponse = new SchemaResponse();
    schemaResponse.setId(1);
    schemaResponse.setSchemaStr(keySchemaStr);
    AbstractAvroStoreClient mockClient = Mockito.mock(AbstractAvroStoreClient.class);
    Mockito.doReturn(storeName).when(mockClient).getStoreName();
    Future<byte[]> mockFuture = Mockito.mock(Future.class);
    Mockito.doReturn(mapper.writeValueAsBytes(schemaResponse)).when(mockFuture).get();
    Mockito.doReturn(mockFuture).when(mockClient).getRaw("key_schema/" + storeName);
    SchemaReader schemaReader = new SchemaReader(mockClient);

    Mockito.doReturn(mapper.writeValueAsBytes(multiSchemaResponse)).when(mockFuture).get();
    Mockito.doReturn(storeName).when(mockClient).getStoreName();
    Mockito.doReturn(mockFuture).when(mockClient).getRaw("value_schema/" + storeName);;
    Assert.assertNull(schemaReader.getLatestValueSchema());
    Mockito.verify(mockClient, Mockito.timeout(TIMEOUT).times(2)).getRaw(Mockito.anyString());
  }

  @Test (expectedExceptions = VeniceClientException.class)
  public void testGetLatestValueSchemaWhenServerError() throws ExecutionException, InterruptedException, VeniceClientException, IOException {
    String storeName = "test_store";
    String keySchemaStr = "\"string\"";
    // setup key schema
    SchemaResponse schemaResponse = new SchemaResponse();
    schemaResponse.setId(1);
    schemaResponse.setSchemaStr(keySchemaStr);
    AbstractAvroStoreClient mockClient = Mockito.mock(AbstractAvroStoreClient.class);
    Mockito.doReturn(storeName).when(mockClient).getStoreName();
    Future<byte[]> mockFuture = Mockito.mock(Future.class);
    Mockito.doReturn(mapper.writeValueAsBytes(schemaResponse)).when(mockFuture).get();
    Mockito.doReturn(mockFuture).when(mockClient).getRaw("key_schema/" + storeName);
    SchemaReader schemaReader = new SchemaReader(mockClient);

    Mockito.doThrow(new ExecutionException(new VeniceServerException("Server error"))).when(mockFuture).get();
    Mockito.doReturn(mockFuture).when(mockClient).getRaw("value_schema/" + storeName);
    schemaReader.getLatestValueSchema();
  }
}
