package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import org.apache.avro.Schema;


/**
 * {@link AvroGenericStoreClient} implementation for Avro generic type.
 */
public class AvroGenericStoreClientImpl<K, V> extends AbstractAvroStoreClient<K, V> {
  public AvroGenericStoreClientImpl(TransportClient transportClient, ClientConfig clientConfig) {
    this(transportClient, true, clientConfig);
  }

  public AvroGenericStoreClientImpl(
      TransportClient transportClient,
      boolean needSchemaReader,
      ClientConfig clientConfig) {
    super(transportClient, needSchemaReader, clientConfig);

    if (clientConfig.isUseFastAvro()) {
      FastSerializerDeserializerFactory.verifyWhetherFastGenericDeserializerWorks();
    }
  }

  /**
   * To avoid cycle dependency, we need to initialize another store client for schema reader.
   */
  @Override
  protected AbstractAvroStoreClient<K, V> getStoreClientForSchemaReader() {
    return new AvroGenericStoreClientImpl<K, V>(
        getTransportClient().getCopyIfNotUsableInCallback(),
        false,
        ClientConfig.defaultGenericClientConfig(getStoreName()));
  }

  @Override
  public RecordDeserializer<V> getDataRecordDeserializer(int writerSchemaId) throws VeniceClientException {
    SchemaReader schemaReader = getSchemaReader();
    Schema writerSchema = schemaReader.getValueSchema(writerSchemaId);
    if (writerSchema == null) {
      throw new VeniceClientException(
          "Failed to get value schema for store: " + getStoreName() + " and id: " + writerSchemaId);
    }

    // Always use the writer schema as the reader schema.
    return getDeserializerFromFactory(writerSchema, writerSchema);
  }

  protected RecordDeserializer<V> getDeserializerFromFactory(Schema writer, Schema reader) {
    if (getClientConfig().isUseFastAvro()) {
      return FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(writer, reader);
    } else {
      return SerializerDeserializerFactory.getAvroGenericDeserializer(writer, reader);
    }
  }

  public String toString() {
    return this.getClass().getSimpleName() + "(transportClient: " + getTransportClient().toString() + ")";
  }
}
