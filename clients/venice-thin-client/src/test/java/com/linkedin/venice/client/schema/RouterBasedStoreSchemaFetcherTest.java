package com.linkedin.venice.client.schema;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AbstractAvroStoreClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.schema.writecompute.WriteComputeSchemaConverter;
import com.linkedin.venice.utils.ObjectMapperFactory;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.avro.Schema;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class RouterBasedStoreSchemaFetcherTest {
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();
  private static final WriteComputeSchemaConverter UPDATE_SCHEMA_CONVERTER = WriteComputeSchemaConverter.getInstance();

  private static final String storeName = "test_store";
  private static final String keySchemaStr = "\"string\"";
  private static final String valueSchemaStr1 =
      "{" + "  \"namespace\" : \"example.avro\",  " + "  \"type\": \"record\",   " + "  \"name\": \"User\",     "
          + "  \"fields\": [           " + "       { \"name\": \"id\", \"type\": \"string\", \"default\": \"id\"},  "
          + "       { \"name\": \"name\", \"type\": \"string\", \"default\": \"id\"}  " + "  ] " + " } ";
  private static final String valueSchemaStr2 =
      "{" + "  \"namespace\" : \"example.avro\",  " + "  \"type\": \"record\",   " + "  \"name\": \"User\",     "
          + "  \"fields\": [           " + "       { \"name\": \"id\", \"type\": \"string\", \"default\": \"id\"},  "
          + "       { \"name\": \"name\", \"type\": \"string\", \"default\": \"id\"},  "
          + "       { \"name\": \"age\", \"type\": \"int\", \"default\": -1 }" + "  ] " + " } ";
  private static final String valueSchemaStr3 =
      "{    \"namespace\": \"example.avro\",    \"type\": \"record\",    \"name\": \"User\",    \"fields\": [        {            \"name\": \"name\",            \"type\": \"string\",            \"default\": \"id\"        },        {            \"name\": \"age\",            \"type\": \"int\",            \"default\": -1        }    ]}";

  private static final int TIMEOUT = 3;

  @Test
  public void testGetKeySchema() throws IOException, ExecutionException, InterruptedException, VeniceClientException {
    AbstractAvroStoreClient mockClient = Mockito.mock(AbstractAvroStoreClient.class);
    Mockito.doReturn(storeName).when(mockClient).getStoreName();
    SchemaResponse schemaResponse = new SchemaResponse();
    schemaResponse.setId(1);
    schemaResponse.setSchemaStr(keySchemaStr);
    CompletableFuture<byte[]> mockFuture = Mockito.mock(CompletableFuture.class);
    Mockito.doReturn(OBJECT_MAPPER.writeValueAsBytes(schemaResponse)).when(mockFuture).get();
    Mockito.doReturn(mockFuture).when(mockClient).getRaw("key_schema/" + storeName);

    StoreSchemaFetcher storeSchemaFetcher = new RouterBasedStoreSchemaFetcher(mockClient);
    Schema schema = storeSchemaFetcher.getKeySchema();
    Assert.assertEquals(schema.toString(), keySchemaStr);
    Schema schema1 = storeSchemaFetcher.getKeySchema();
    Assert.assertEquals(schema1, schema);
    // Must be the different Schema instance
    Assert.assertNotSame(schema, schema1);
    Mockito.verify(mockClient, Mockito.timeout(TIMEOUT).times(2)).getRaw(Mockito.anyString());
  }

  @Test
  public void testGetLatestValueSchema()
      throws IOException, ExecutionException, InterruptedException, VeniceClientException {
    AbstractAvroStoreClient mockClient = Mockito.mock(AbstractAvroStoreClient.class);
    Mockito.doReturn(storeName).when(mockClient).getStoreName();
    // Set up mocks for value schemas
    CompletableFuture<byte[]> mockFutureSupersetSchema = Mockito.mock(CompletableFuture.class);
    Mockito.doReturn(OBJECT_MAPPER.writeValueAsBytes(createSingleSchemaResponse(2, valueSchemaStr2)))
        .when(mockFutureSupersetSchema)
        .get();
    Mockito.doReturn(mockFutureSupersetSchema).when(mockClient).getRaw("latest_value_schema/" + storeName);

    CompletableFuture<byte[]> mockFutureValueSchema = Mockito.mock(CompletableFuture.class);
    Mockito.doReturn(OBJECT_MAPPER.writeValueAsBytes(createValueSchemaMultiSchemaResponse()))
        .when(mockFutureValueSchema)
        .get();
    Mockito.doReturn(mockFutureValueSchema).when(mockClient).getRaw("value_schema/" + storeName);

    // Get latest value schema twice.
    StoreSchemaFetcher storeSchemaFetcher = new RouterBasedStoreSchemaFetcher(mockClient);
    Schema valueSchema1 = storeSchemaFetcher.getLatestValueSchema();
    Schema valueSchema2 = storeSchemaFetcher.getLatestValueSchema();

    // Each invocation should fetch the latest schema, but it is expected the object to not be the same as we don't
    // implement cache.
    Schema expectedValueSchema = Schema.parse(valueSchemaStr2);
    Assert.assertEquals(valueSchema1, expectedValueSchema);
    Assert.assertEquals(valueSchema2, valueSchema1);
    Assert.assertNotSame(valueSchema1, valueSchema2);
    Mockito.verify(mockClient, Mockito.timeout(TIMEOUT).times(2)).getRaw(Mockito.anyString());
  }

  @Test
  public void testGetAllValueSchemas()
      throws IOException, ExecutionException, InterruptedException, VeniceClientException {
    AbstractAvroStoreClient mockClient = Mockito.mock(AbstractAvroStoreClient.class);
    Mockito.doReturn(storeName).when(mockClient).getStoreName();
    // Set up mocks for value schemas
    CompletableFuture<byte[]> mockFuture = Mockito.mock(CompletableFuture.class);
    Mockito.doReturn(OBJECT_MAPPER.writeValueAsBytes(createValueSchemaMultiSchemaResponse())).when(mockFuture).get();
    Mockito.doReturn(mockFuture).when(mockClient).getRaw("value_schema/" + storeName);

    // Get all value schemas
    StoreSchemaFetcher storeSchemaFetcher = new RouterBasedStoreSchemaFetcher(mockClient);
    Set<Schema> allSchemas = storeSchemaFetcher.getAllValueSchemas();

    Assert.assertEquals(allSchemas.size(), 3);
    Assert.assertTrue(allSchemas.contains(Schema.parse(valueSchemaStr1)));
    Assert.assertTrue(allSchemas.contains(Schema.parse(valueSchemaStr2)));
    Assert.assertTrue(allSchemas.contains(Schema.parse(valueSchemaStr3)));
    Mockito.verify(mockClient, Mockito.timeout(TIMEOUT).times(1)).getRaw(Mockito.anyString());
  }

  @Test
  public void testGetLatestValueSchemaWhenRouterDoesntSupportAPI()
      throws IOException, ExecutionException, InterruptedException, VeniceClientException {
    AbstractAvroStoreClient mockClient = Mockito.mock(AbstractAvroStoreClient.class);
    Mockito.doReturn(storeName).when(mockClient).getStoreName();
    // Set up mocks for value schemas
    CompletableFuture<byte[]> mockFutureSupersetSchema = Mockito.mock(CompletableFuture.class);
    Mockito.doThrow(new VeniceException()).when(mockFutureSupersetSchema).get();
    Mockito.doReturn(mockFutureSupersetSchema).when(mockClient).getRaw("latest_value_schema/" + storeName);

    CompletableFuture<byte[]> mockFutureValueSchema = Mockito.mock(CompletableFuture.class);
    Mockito.doReturn(OBJECT_MAPPER.writeValueAsBytes(createValueSchemaMultiSchemaResponse()))
        .when(mockFutureValueSchema)
        .get();
    Mockito.doReturn(mockFutureValueSchema).when(mockClient).getRaw("value_schema/" + storeName);

    // Get latest value schema twice.
    StoreSchemaFetcher storeSchemaFetcher = new RouterBasedStoreSchemaFetcher(mockClient);
    Schema valueSchema1 = storeSchemaFetcher.getLatestValueSchema();
    Schema valueSchema2 = storeSchemaFetcher.getLatestValueSchema();

    // Each invocation should fetch the latest schema, but it is expected the object to not be the same as we don't
    // implement cache.
    Schema expectedValueSchema = Schema.parse(valueSchemaStr2);
    Assert.assertEquals(valueSchema1, expectedValueSchema);
    Assert.assertEquals(valueSchema2, valueSchema1);
    Assert.assertNotSame(valueSchema1, valueSchema2);

    // Each call to get the latest schema first tries to query /latest_value_schema and then /value_schema endpoints
    Mockito.verify(mockClient, Mockito.timeout(TIMEOUT).times(4)).getRaw(Mockito.anyString());
  }

  @Test
  public void testGetUpdateSchema()
      throws IOException, ExecutionException, InterruptedException, VeniceClientException {
    AbstractAvroStoreClient mockClient = Mockito.mock(AbstractAvroStoreClient.class);
    Mockito.doReturn(storeName).when(mockClient).getStoreName();
    // Set up mocks for value schemas
    CompletableFuture<byte[]> mockGetAllValueSchemaFuture = Mockito.mock(CompletableFuture.class);
    Mockito.doReturn(OBJECT_MAPPER.writeValueAsBytes(createValueSchemaMultiSchemaResponse()))
        .when(mockGetAllValueSchemaFuture)
        .get();
    Mockito.doReturn(mockGetAllValueSchemaFuture).when(mockClient).getRaw("value_schema/" + storeName);
    // Set up mocks for update schemas
    CompletableFuture<byte[]> mockGetUpdateValueSchemaFuture1 = Mockito.mock(CompletableFuture.class);
    Mockito.doReturn(OBJECT_MAPPER.writeValueAsBytes(createUpdateSchemaResponse(1, 1, valueSchemaStr1)))
        .when(mockGetUpdateValueSchemaFuture1)
        .get();
    Mockito.doReturn(mockGetUpdateValueSchemaFuture1).when(mockClient).getRaw("update_schema/" + storeName + "/1");
    CompletableFuture<byte[]> mockGetUpdateValueSchemaFuture2 = Mockito.mock(CompletableFuture.class);
    Mockito.doReturn(OBJECT_MAPPER.writeValueAsBytes(createUpdateSchemaResponse(2, 1, valueSchemaStr2)))
        .when(mockGetUpdateValueSchemaFuture2)
        .get();
    Mockito.doReturn(mockGetUpdateValueSchemaFuture2).when(mockClient).getRaw("update_schema/" + storeName + "/2");
    CompletableFuture<byte[]> mockGetUpdateValueSchemaFuture3 = Mockito.mock(CompletableFuture.class);
    Mockito.doReturn(OBJECT_MAPPER.writeValueAsBytes(createUpdateSchemaResponse(3, 1, valueSchemaStr3)))
        .when(mockGetUpdateValueSchemaFuture3)
        .get();
    Mockito.doReturn(mockGetUpdateValueSchemaFuture3).when(mockClient).getRaw("update_schema/" + storeName + "/3");

    // Fetch both update schemas.
    StoreSchemaFetcher storeSchemaFetcher = new RouterBasedStoreSchemaFetcher(mockClient);
    Schema updateSchema1 = storeSchemaFetcher.getUpdateSchema(Schema.parse(valueSchemaStr1));
    Schema updateSchema2 = storeSchemaFetcher.getUpdateSchema(Schema.parse(valueSchemaStr2));
    Schema updateSchema3 = storeSchemaFetcher.getUpdateSchema(Schema.parse(valueSchemaStr3));
    Assert.assertEquals(updateSchema1, UPDATE_SCHEMA_CONVERTER.convert(valueSchemaStr1));
    Assert.assertEquals(updateSchema2, UPDATE_SCHEMA_CONVERTER.convert(valueSchemaStr2));
    Assert.assertEquals(updateSchema3, UPDATE_SCHEMA_CONVERTER.convert(valueSchemaStr3));
    // Each update schema fetch should call get value schemas first then get latest update schema.
    Mockito.verify(mockClient, Mockito.timeout(TIMEOUT).times(6)).getRaw(Mockito.anyString());
  }

  private MultiSchemaResponse createValueSchemaMultiSchemaResponse() {
    MultiSchemaResponse multiSchemaResponse = new MultiSchemaResponse();
    MultiSchemaResponse.Schema[] schemas = new MultiSchemaResponse.Schema[3];

    MultiSchemaResponse.Schema schema1 = new MultiSchemaResponse.Schema();
    schema1.setId(1);
    schema1.setSchemaStr(valueSchemaStr1);
    schemas[0] = schema1;

    MultiSchemaResponse.Schema schema2 = new MultiSchemaResponse.Schema();
    schema2.setId(2);
    schema2.setSchemaStr(valueSchemaStr2);
    schemas[1] = schema2;

    MultiSchemaResponse.Schema schema3 = new MultiSchemaResponse.Schema();
    schema3.setId(3);
    schema3.setSchemaStr(valueSchemaStr3);
    schemas[2] = schema3;

    multiSchemaResponse.setSchemas(schemas);
    multiSchemaResponse.setSuperSetSchemaId(2);
    return multiSchemaResponse;
  }

  private SchemaResponse createUpdateSchemaResponse(int valueSchemaId, int derivedSchemaId, String valueSchemaStr) {
    SchemaResponse schemaResponse = new SchemaResponse();
    schemaResponse.setId(valueSchemaId);
    schemaResponse.setDerivedSchemaId(derivedSchemaId);
    schemaResponse.setSchemaStr(UPDATE_SCHEMA_CONVERTER.convert(valueSchemaStr).toString());
    return schemaResponse;
  }

  private SchemaResponse createSingleSchemaResponse(int schemaId, String schemaStr) {
    SchemaResponse schemaResponse = new SchemaResponse();
    schemaResponse.setId(schemaId);
    schemaResponse.setSchemaStr(schemaStr);
    return schemaResponse;
  }
}
