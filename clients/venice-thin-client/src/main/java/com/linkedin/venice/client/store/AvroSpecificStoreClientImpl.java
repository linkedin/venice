package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.serializer.FastSerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;


/**
 * {@link AvroSpecificStoreClient} implementation for Avro SpecificRecord.
 * @param <V>
 */
public class AvroSpecificStoreClientImpl<K, V extends SpecificRecord> extends AbstractAvroStoreClient<K, V>
    implements AvroSpecificStoreClient<K, V> {
  private final Class<V> valueClass;

  public AvroSpecificStoreClientImpl(TransportClient transportClient, ClientConfig clientConfig) {
    this(transportClient, true, clientConfig);
  }

  private AvroSpecificStoreClientImpl(
      TransportClient transportClient,
      boolean needSchemaReader,
      ClientConfig clientConfig) {
    super(transportClient, needSchemaReader, clientConfig);
    valueClass = clientConfig.getSpecificValueClass();

    if (getClientConfig().isUseFastAvro()) {
      FastSerializerDeserializerFactory.verifyWhetherFastSpecificDeserializerWorks(valueClass);
    }
  }

  @Override
  public void start() {
    super.start();
  }

  @Override
  public RecordDeserializer<V> getDataRecordDeserializer(int schemaId) throws VeniceClientException {
    SchemaReader schemaReader = getSchemaReader();
    Schema writeSchema = schemaReader.getValueSchema(schemaId);
    if (writeSchema == null) {
      throw new VeniceClientException(
          "Failed to get value schema for store: " + getStoreName() + " and id: " + schemaId);
    }
    if (getClientConfig().isUseFastAvro()) {
      return FastSerializerDeserializerFactory.getFastAvroSpecificDeserializer(writeSchema, valueClass);
    } else {
      return SerializerDeserializerFactory.getAvroSpecificDeserializer(writeSchema, valueClass);
    }
  }

  @Override
  protected Optional<Schema> getReaderSchema() {
    return Optional.of(SpecificData.get().getSchema(valueClass));
  }
}
