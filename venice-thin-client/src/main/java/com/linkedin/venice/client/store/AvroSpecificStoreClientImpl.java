package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.schema.SchemaReader;
import com.linkedin.venice.client.store.deserialization.BatchGetDeserializerType;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.serializer.RecordDeserializer;
import java.util.concurrent.Executor;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

/**
 * {@link AvroSpecificStoreClient} implementation for Avro SpecificRecord.
 * @param <V>
 */
public class AvroSpecificStoreClientImpl<K, V extends SpecificRecord>
    extends AbstractAvroStoreClient<K, V> implements AvroSpecificStoreClient<K, V> {
  private Class<V> valueClass;

  public AvroSpecificStoreClientImpl(TransportClient transportClient, ClientConfig clientConfig) {
    this(transportClient, true, clientConfig);
  }

  private AvroSpecificStoreClientImpl(TransportClient transportClient,
                                     boolean needSchemaReader,
                                     ClientConfig clientConfig) {
    super(transportClient, needSchemaReader, clientConfig);
    this.valueClass = clientConfig.getSpecificValueClass();
  }

  @Override
  public RecordDeserializer<V> getDataRecordDeserializer(int schemaId) throws VeniceClientException {
    SchemaReader schemaReader = getSchemaReader();
    Schema writeSchema = schemaReader.getValueSchema(schemaId);
    if (null == writeSchema) {
      throw new VeniceClientException("Failed to get value schema for store: " + getStoreName() + " and id: " + schemaId);
    }
    return SerializerDeserializerFactory.getAvroSpecificDeserializer(writeSchema, valueClass);
  }

  /**
   * To avoid cycle dependency, we need to initialize another store client for schema reader.
   * @return
   * @throws VeniceClientException
   */
  @Override
  protected AbstractAvroStoreClient<K, V> getStoreClientForSchemaReader() {
    return new AvroSpecificStoreClientImpl<K, V>(getTransportClient().getCopyIfNotUsableInCallback(),
        false, ClientConfig.defaultSpecificClientConfig(getStoreName(), valueClass));
  }
}
