package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.schema.SchemaReader;
import com.linkedin.venice.client.store.deserialization.BatchGetDeserializerType;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import java.util.concurrent.Executor;
import org.apache.avro.Schema;

/**
 * {@link AvroGenericStoreClient} implementation for Avro generic type.
 */
public class AvroGenericStoreClientImpl<K, V> extends AbstractAvroStoreClient<K, V> {
  public AvroGenericStoreClientImpl(TransportClient transportClient, ClientConfig clientConfig) {
    this(transportClient, true, clientConfig);
  }

  protected AvroGenericStoreClientImpl(TransportClient transportClient, boolean needSchemaReader, ClientConfig clientConfig) {
    super(transportClient, needSchemaReader, clientConfig);

    if (isUseFastAvro()) {
      FastSerializerDeserializerFactory.verifyWhetherFastGenericSerializerWorks();
    }
  }

  /**
   * To avoid cycle dependency, we need to initialize another store client for schema reader.
   */
  @Override
  protected AbstractAvroStoreClient<K, V> getStoreClientForSchemaReader() {
    return new AvroGenericStoreClientImpl<K, V>(getTransportClient().getCopyIfNotUsableInCallback(),
        false, ClientConfig.defaultGenericClientConfig(getStoreName()));
  }

  @Override
  public RecordDeserializer<V> getDataRecordDeserializer(int schemaId) throws VeniceClientException {
    SchemaReader schemaReader = getSchemaReader();
    // Get latest value schema
    Schema readerSchema = schemaReader.getLatestValueSchema();
    if (null == readerSchema) {
      throw new VeniceClientException("Failed to get latest value schema for store: " + getStoreName());
    }
    Schema writerSchema = schemaReader.getValueSchema(schemaId);
    if (null == writerSchema) {
      throw new VeniceClientException("Failed to get value schema for store: " + getStoreName() + " and id: " + schemaId);
    }

    return getDeserializerFromFactory(writerSchema, readerSchema);
  }

  protected RecordDeserializer<V> getDeserializerFromFactory(Schema writer, Schema reader) {
    if (isUseFastAvro()) {
      return FastSerializerDeserializerFactory.getFastAvroGenericDeserializer(writer, reader);
    } else {
      return SerializerDeserializerFactory.getAvroGenericDeserializer(writer, reader);
    }
  }

  public String toString() {
    return this.getClass().getSimpleName() + "(transportClient: " + getTransportClient().toString() + ")";
  }
}
