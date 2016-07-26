package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.schema.SchemaReader;
import com.linkedin.venice.client.serializer.AvroSerializerDeserializerFactory;
import com.linkedin.venice.client.serializer.RecordDeserializer;
import com.linkedin.venice.client.store.transport.TransportClient;
import org.apache.avro.Schema;

/**
 * {@link AvroGenericStoreClient} implementation for Avro generic type.
 * @param <V>
 */
public class AvroGenericStoreClientImpl<V> extends AbstractAvroStoreClient<V> {
  public AvroGenericStoreClientImpl(TransportClient<V> transportClient, String storeName) throws VeniceClientException {
    this(transportClient, storeName, true);
  }
  private AvroGenericStoreClientImpl(TransportClient<V> transportClient, String storeName, boolean needSchemaReader) throws VeniceClientException {
    super(transportClient, storeName, needSchemaReader);
  }

  /**
   * To avoid cycle dependency, we need to initialize another store client for schema reader.
   * @return
   * @throws VeniceClientException
   */
  @Override
  protected AbstractAvroStoreClient<V> getStoreClientForSchemaReader() throws VeniceClientException {
    return new AvroGenericStoreClientImpl<V>(getTransportClient().getCopyIfNotUsableInCallback(), getStoreName(), false);
  }

  @Override
  public RecordDeserializer<V> fetch(int schemaId) throws VeniceClientException {
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
    return AvroSerializerDeserializerFactory.getAvroGenericDeserializer(writerSchema, readerSchema);
  }
}
