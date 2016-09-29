package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.schema.SchemaReader;
import com.linkedin.venice.client.serializer.AvroSerializerDeserializerFactory;
import com.linkedin.venice.client.serializer.RecordDeserializer;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.stats.TehutiUtils;
import io.tehuti.metrics.MetricsRepository;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

/**
 * {@link AvroSpecificStoreClient} implementation for Avro SpecificRecord.
 * @param <V>
 */
public class AvroSpecificStoreClientImpl<V extends SpecificRecord> extends AbstractAvroStoreClient<V> implements AvroSpecificStoreClient<V> {
  private Class<V> valueClass;
  public AvroSpecificStoreClientImpl(TransportClient<V> transportClient,
                                     String storeName,
                                     Class<V> c
                                     ) {
    this(transportClient, storeName, c, true, AbstractAvroStoreClient.getDeafultClientMetricsRepository(storeName));
  }

  public AvroSpecificStoreClientImpl(TransportClient<V> transportClient,
                                     String storeName,
                                     Class<V> c,
                                     MetricsRepository metricsRepository) {
    this(transportClient, storeName, c, true, metricsRepository);
  }

  private AvroSpecificStoreClientImpl(TransportClient<V> transportClient,
                                      String storeName,
                                      Class<V> c,
                                      boolean needSchemaReader,
                                      MetricsRepository metricsRepository) {
    super(transportClient, storeName, needSchemaReader, metricsRepository);
    this.valueClass = c;
  }

  @Override
  public RecordDeserializer<V> fetch(int schemaId) throws VeniceClientException {
    SchemaReader schemaReader = getSchemaReader();
    Schema writeSchema = schemaReader.getValueSchema(schemaId);
    if (null == writeSchema) {
      throw new VeniceClientException("Failed to get value schema for store: " + getStoreName() + " and id: " + schemaId);
    }
    return AvroSerializerDeserializerFactory.getAvroSpecificDeserializer(writeSchema, valueClass);
  }

  /**
   * To avoid cycle dependency, we need to initialize another store client for schema reader.
   * @return
   * @throws VeniceClientException
   */
  @Override
  protected AbstractAvroStoreClient<V> getStoreClientForSchemaReader() {
    return new AvroSpecificStoreClientImpl<V>(getTransportClient()
        .getCopyIfNotUsableInCallback(), getStoreName(), valueClass, false, getMetricsRepository());
  }
}
