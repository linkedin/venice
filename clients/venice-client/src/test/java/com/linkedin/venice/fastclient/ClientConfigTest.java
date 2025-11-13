package com.linkedin.venice.fastclient;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.RecordSerializer;
import java.util.Optional;
import org.apache.avro.Schema;
import org.testng.annotations.Test;


public class ClientConfigTest {
  private ClientConfig.ClientConfigBuilder getClientConfigWithMinimumRequiredInputs() {
    return new ClientConfig.ClientConfigBuilder<>().setStoreName("test_store")
        .setR2Client(mock(Client.class))
        .setD2Client(mock(D2Client.class))
        .setClusterDiscoveryD2Service("test_server_discovery");
  }

  @Test(expectedExceptions = VeniceClientException.class, expectedExceptionsMessageRegExp = "storeName param shouldn't be empty")
  public void testClientWithNoStoreName() {
    new ClientConfig.ClientConfigBuilder<>().build();
  }

  @Test(expectedExceptions = VeniceClientException.class, expectedExceptionsMessageRegExp = "storeName param shouldn't be empty")
  public void testClientWithEmptyStoreName() {
    new ClientConfig.ClientConfigBuilder<>().setStoreName("").build();
  }

  @Test(expectedExceptions = VeniceClientException.class, expectedExceptionsMessageRegExp = "r2Client param shouldn't be null")
  public void testClientWithoutR2Client() {
    new ClientConfig.ClientConfigBuilder<>().setStoreName("test_store").build();
  }

  @Test
  public void testClientWithAllRequiredInputs() {
    ClientConfig.ClientConfigBuilder clientConfigBuilder = getClientConfigWithMinimumRequiredInputs();
    clientConfigBuilder.build();
  }

  @Test(expectedExceptions = VeniceClientException.class, expectedExceptionsMessageRegExp = "Either param: specificThinClient or param: genericThinClient.*")
  public void testClientWithDualReadAndNoThinClients() {
    ClientConfig.ClientConfigBuilder clientConfigBuilder = getClientConfigWithMinimumRequiredInputs();
    clientConfigBuilder.setDualReadEnabled(true);
    clientConfigBuilder.build();
  }

  @Test(expectedExceptions = VeniceClientException.class, expectedExceptionsMessageRegExp = "Both param: specificThinClient and param: genericThinClient should not be specified.*")
  public void testClientWithOutDualReadButWithThinClients() {
    ClientConfig.ClientConfigBuilder clientConfigBuilder = getClientConfigWithMinimumRequiredInputs();
    clientConfigBuilder.setGenericThinClient(mock(AvroGenericStoreClient.class));
    clientConfigBuilder.build();
  }

  @Test(expectedExceptions = VeniceClientException.class, expectedExceptionsMessageRegExp = "longTailRetryThresholdForSingleGetInMicroSeconds must be positive.*")
  public void testClientWithInvalidLongTailRetryThresholdForSingleGet() {
    ClientConfig.ClientConfigBuilder clientConfigBuilder = getClientConfigWithMinimumRequiredInputs();
    clientConfigBuilder.setLongTailRetryEnabledForSingleGet(true);
    clientConfigBuilder.setLongTailRetryThresholdForSingleGetInMicroSeconds(0);
    clientConfigBuilder.build();
  }

  @Test
  public void testLongTailRetryWithDualRead() {
    ClientConfig.ClientConfigBuilder clientConfigBuilder = getClientConfigWithMinimumRequiredInputs();
    clientConfigBuilder.setDualReadEnabled(true)
        .setGenericThinClient(mock(AvroGenericStoreClient.class))
        .setLongTailRetryEnabledForSingleGet(true)
        .setLongTailRetryThresholdForSingleGetInMicroSeconds(1000)
        .build();
  }

  @Test
  public void testDefaultBatchGetRetryThresholds() {
    ClientConfig.ClientConfigBuilder clientConfigBuilder = getClientConfigWithMinimumRequiredInputs();
    ClientConfig clientConfig = clientConfigBuilder.build();
    assertEquals(
        clientConfig.getLongTailRangeBasedRetryThresholdForBatchGetInMilliSeconds(),
        "1-12:8,13-20:30,21-150:50,151-500:100,501-:500");
  }

  @Test
  public void testDefaulComputeRetryThresholds() {
    ClientConfig.ClientConfigBuilder clientConfigBuilder = getClientConfigWithMinimumRequiredInputs();
    ClientConfig clientConfig = clientConfigBuilder.build();
    assertEquals(
        clientConfig.getLongTailRangeBasedRetryThresholdForComputeInMilliSeconds(),
        "1-12:8,13-20:30,21-150:50,151-500:100,501-:500");
  }

  @Test
  public void testClientConfigWithCustomKeySerializerFactory() {
    SerializerFactory mockSerializerFactory = mock(SerializerFactory.class);
    RecordSerializer mockSerializer = mock(RecordSerializer.class);
    Schema mockSchema = mock(Schema.class);
    when(mockSerializerFactory.createSerializer(mockSchema)).thenReturn(mockSerializer);

    ClientConfig.ClientConfigBuilder clientConfigBuilder = getClientConfigWithMinimumRequiredInputs();
    clientConfigBuilder.setKeySerializerFactory(mockSerializerFactory);
    ClientConfig clientConfig = clientConfigBuilder.build();

    // Verify the factory is present
    assertTrue(clientConfig.getKeySerializerFactory().isPresent());
    assertEquals(clientConfig.getKeySerializerFactory().get(), mockSerializerFactory);

    // Verify the factory works
    Optional<SerializerFactory> factoryOptional = clientConfig.getKeySerializerFactory();
    assertTrue(factoryOptional.isPresent());
    RecordSerializer serializer = factoryOptional.get().createSerializer(mockSchema);
    assertEquals(serializer, mockSerializer);
  }

  @Test
  public void testClientConfigWithCustomValueDeserializerFactory() {
    DeserializerFactory mockDeserializerFactory = mock(DeserializerFactory.class);
    RecordDeserializer mockDeserializer = mock(RecordDeserializer.class);
    Schema writerSchema = mock(Schema.class);
    when(mockDeserializerFactory.createDeserializer(writerSchema)).thenReturn(mockDeserializer);

    ClientConfig.ClientConfigBuilder clientConfigBuilder = getClientConfigWithMinimumRequiredInputs();
    clientConfigBuilder.setValueDeserializerFactory(mockDeserializerFactory);
    ClientConfig clientConfig = clientConfigBuilder.build();

    // Verify the factory is present
    assertTrue(clientConfig.getValueDeserializerFactory().isPresent());
    assertEquals(clientConfig.getValueDeserializerFactory().get(), mockDeserializerFactory);

    // Verify the factory works
    Optional<DeserializerFactory> factoryOptional = clientConfig.getValueDeserializerFactory();
    assertTrue(factoryOptional.isPresent());
    RecordDeserializer deserializer = factoryOptional.get().createDeserializer(writerSchema);
    assertEquals(deserializer, mockDeserializer);
  }

  @Test
  public void testClientConfigWithBothCustomFactories() {
    SerializerFactory mockSerializerFactory = mock(SerializerFactory.class);
    DeserializerFactory mockDeserializerFactory = mock(DeserializerFactory.class);

    ClientConfig.ClientConfigBuilder clientConfigBuilder = getClientConfigWithMinimumRequiredInputs();
    clientConfigBuilder.setKeySerializerFactory(mockSerializerFactory);
    clientConfigBuilder.setValueDeserializerFactory(mockDeserializerFactory);
    ClientConfig clientConfig = clientConfigBuilder.build();

    // Verify both factories are present
    assertTrue(clientConfig.getKeySerializerFactory().isPresent());
    assertTrue(clientConfig.getValueDeserializerFactory().isPresent());
    assertEquals(clientConfig.getKeySerializerFactory().get(), mockSerializerFactory);
    assertEquals(clientConfig.getValueDeserializerFactory().get(), mockDeserializerFactory);
  }

  @Test
  public void testClientConfigWithoutCustomFactories() {
    // When no custom factories are set, they should be empty optionals
    ClientConfig.ClientConfigBuilder clientConfigBuilder = getClientConfigWithMinimumRequiredInputs();
    ClientConfig clientConfig = clientConfigBuilder.build();

    assertFalse(clientConfig.getKeySerializerFactory().isPresent());
    assertFalse(clientConfig.getValueDeserializerFactory().isPresent());
  }

  @Test
  public void testClientConfigBuilderClonePreservesFactories() {
    SerializerFactory mockSerializerFactory = mock(SerializerFactory.class);
    DeserializerFactory mockDeserializerFactory = mock(DeserializerFactory.class);

    ClientConfig.ClientConfigBuilder originalBuilder = getClientConfigWithMinimumRequiredInputs();
    originalBuilder.setKeySerializerFactory(mockSerializerFactory);
    originalBuilder.setValueDeserializerFactory(mockDeserializerFactory);

    // Clone the builder
    ClientConfig.ClientConfigBuilder clonedBuilder = originalBuilder.clone();
    ClientConfig clonedConfig = clonedBuilder.build();

    // Verify factories are preserved in the clone
    assertTrue(clonedConfig.getKeySerializerFactory().isPresent());
    assertTrue(clonedConfig.getValueDeserializerFactory().isPresent());
    assertEquals(clonedConfig.getKeySerializerFactory().get(), mockSerializerFactory);
    assertEquals(clonedConfig.getValueDeserializerFactory().get(), mockDeserializerFactory);
  }
}
