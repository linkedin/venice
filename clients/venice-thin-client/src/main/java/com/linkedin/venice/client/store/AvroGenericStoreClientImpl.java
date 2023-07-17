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

  @Override
  public RecordDeserializer<V> getDataRecordDeserializer(int writerSchemaId) throws VeniceClientException {
    SchemaReader schemaReader = getSchemaReader();

    // Get latest value schema
    Schema readerSchema = schemaReader.getLatestValueSchema();
    if (readerSchema == null) {
      throw new VeniceClientException("Failed to get latest value schema for store: " + getStoreName());
    }

    Schema writerSchema = schemaReader.getValueSchema(writerSchemaId);
    if (writerSchema == null) {
      throw new VeniceClientException(
          "Failed to get value schema for store: " + getStoreName() + " and id: " + writerSchemaId);
    }

    /**
     * The reason to fetch the latest value schema before fetching the writer schema since internally
     * it will fetch all the available value schemas when no value schema is present in {@link SchemaReader},
     * which means the latest value schema could be pretty accurate even the following read requests are
     * asking for older schema versions.
     *
     * The reason to fetch latest value schema again after fetching the writer schema is that the new fetched
     * writer schema could be newer than the cached value schema versions.
     * When the latest value schema is present in {@link SchemaReader}, the following invocation is very cheap.
      */
    readerSchema = schemaReader.getLatestValueSchema();

    return getDeserializerFromFactory(writerSchema, readerSchema);
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
