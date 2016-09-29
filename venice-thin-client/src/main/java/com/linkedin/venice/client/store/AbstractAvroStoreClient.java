package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.schema.SchemaReader;
import com.linkedin.venice.client.serializer.AvroSerializerDeserializerFactory;
import com.linkedin.venice.client.serializer.RecordSerializer;
import com.linkedin.venice.client.store.transport.D2TransportClient;
import com.linkedin.venice.client.store.transport.HttpTransportClient;
import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.client.store.transport.TransportClient;
import com.linkedin.venice.stats.TehutiUtils;
import io.tehuti.metrics.MetricsRepository;
import io.tehuti.utils.Time;
import org.apache.commons.io.IOUtils;

import javax.validation.constraints.NotNull;
import java.util.Base64;
import java.util.concurrent.Future;

public abstract class AbstractAvroStoreClient<V> implements AvroGenericStoreClient<V>, DeserializerFetcher<V> {
  public static final String STORAGE_TYPE = "storage";
  public static final String B64_FORMAT = "?f=b64";
  public static final String VENICE_CLIENT_NAME = "venice_client";
  public static final int TIMEOUT_IN_SECOND = 50;

  private final MetricsRepository metricsRepository;
  private final ClientStats stats;

  /** Encoder to encode store key object */
  private final Base64.Encoder encoder = Base64.getUrlEncoder();

  private final Boolean needSchemaReader;
  /** Used to communicate with Venice backend to retrieve necessary store schemas */
  private SchemaReader schemaReader;
  // Key serializer
  protected RecordSerializer<Object> keySerializer;

  private TransportClient<V> transportClient;
  private String storeName;

  //TODO: build a MetricsRepositoryFactory so that we don't have to pass metricsRepository into it as a param.
  public AbstractAvroStoreClient(TransportClient<V> transportClient,
                                 String storeName,
                                 boolean needSchemaReader,
                                 MetricsRepository metricsRepository) {
    this.metricsRepository = metricsRepository;

    ClientStats.init(this.metricsRepository);
    stats = ClientStats.getInstance();

    this.transportClient = transportClient;
    this.storeName = storeName;
    this.needSchemaReader = needSchemaReader;

    // Set deserializer fetcher for transport client
    transportClient.setDeserializerFetcher(this);
  }

  public String getStoreName() {
    return storeName;
  }

  protected TransportClient<V> getTransportClient() {
    return transportClient;
  }

  @NotNull
  protected SchemaReader getSchemaReader() {
    return schemaReader;
  }

  private String getRequestPathByStoreKey(byte[] key) {
    String b64key = encoder.encodeToString(key);
    return STORAGE_TYPE +
        "/" + storeName +
        "/" + b64key + B64_FORMAT;
  }

  // For testing
  public String getRequestPathByKey(Object key) throws VeniceClientException {
    byte[] serializedKey = keySerializer.serialize(key);
    return getRequestPathByStoreKey(serializedKey);
  }

  @Override
  public Future<V> get(Object key) throws VeniceClientException {
    long startTime = System.currentTimeMillis();
    stats.recordRequest();
    byte[] serializedKey = keySerializer.serialize(key);
    String requestPath = getRequestPathByStoreKey(serializedKey);

    return transportClient.get(requestPath, new ClientCallback() {
      @Override
      public void executeOnSuccess() {
        if (System.currentTimeMillis() - startTime > TIMEOUT_IN_SECOND * Time.MS_PER_SECOND) {
          executeOnError();
        } else {
          stats.recordHealthyRequest();
          stats.recordHealthyLatency(System.currentTimeMillis() - startTime);
        }
      }

      @Override
      public void executeOnError() {
        stats.recordUnhealthyRequest();
        stats.recordUnhealthyLatency(System.currentTimeMillis() - startTime);
      }
    });
  }

  public Future<byte[]> getRaw(String requestPath) {
    return transportClient.getRaw(requestPath);
  }

  @Override
  public void start() throws VeniceClientException {
    if (needSchemaReader) {
      //TODO: remove the 'instanceof' statement once HttpCient got refactored.
      if (transportClient instanceof D2TransportClient) {
        this.schemaReader = new SchemaReader(this);
      } else {
        this.schemaReader = new SchemaReader(this.getStoreClientForSchemaReader());
      }

      // init key serializer
      this.keySerializer =
        AvroSerializerDeserializerFactory.getAvroGenericSerializer(schemaReader.getKeySchema());
    } else {
      this.schemaReader = null;
    }
  }

  @Override
  public void close() {
    boolean isHttp = transportClient instanceof HttpTransportClient;
    IOUtils.closeQuietly(transportClient);
    if (isHttp) { // TODO make d2client close method idempotent.  d2client re-uses the transport client for the schema reader
      IOUtils.closeQuietly(schemaReader);
    }
  }

  protected abstract AbstractAvroStoreClient<V> getStoreClientForSchemaReader();

  static protected MetricsRepository getDeafultClientMetricsRepository(String storeName) {
    return TehutiUtils.getMetricsRepository(VENICE_CLIENT_NAME + "_" + storeName);
  }

  protected MetricsRepository getMetricsRepository() {
    return this.metricsRepository;
  }
}
