package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.schema.SchemaReader;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import java.util.concurrent.Executor;
import org.apache.avro.Schema;

/**
 * {@link AvroGenericStoreClient} implementation for Avro generic type.
 * @param <V>
 */
public class AvroGenericStoreClientImpl<K, V> extends AbstractAvroStoreClient<K, V> {
  public AvroGenericStoreClientImpl(TransportClient transportClient, String storeName) {
    this(transportClient, storeName, true, AbstractAvroStoreClient.getDefaultDeserializationExecutor());
  }

  public AvroGenericStoreClientImpl(TransportClient transportClient, String storeName, Executor deserializationExecutor) {
    this(transportClient, storeName, true, deserializationExecutor);
  }

  protected AvroGenericStoreClientImpl(TransportClient transportClient, String storeName,
                                     boolean needSchemaReader, Executor deserializationExecutor) {
    super(transportClient, storeName, needSchemaReader, deserializationExecutor);
  }

  /**
   * To avoid cycle dependency, we need to initialize another store client for schema reader.
   * @return
   * @throws VeniceClientException
   */
  @Override
  protected AbstractAvroStoreClient<K, V> getStoreClientForSchemaReader() {
    return new AvroGenericStoreClientImpl<K, V>(getTransportClient().getCopyIfNotUsableInCallback(),
        getStoreName(), false, getDeserializationExecutor());
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
    return SerializerDeserializerFactory.getAvroGenericDeserializer(writer, reader);
  }

  public String toString() {
    return this.getClass().getSimpleName() + "(transportClient: " + getTransportClient().toString() + ")";
  }
}
