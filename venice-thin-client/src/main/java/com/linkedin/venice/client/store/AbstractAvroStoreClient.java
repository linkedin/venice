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

  //TODO: do we want it to be configurable?
  public static final int TIMEOUT_IN_SECOND = 5;

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

  public AbstractAvroStoreClient(TransportClient<V> transportClient,
                                 String storeName,
                                 boolean needSchemaReader,
                                 MetricsRepository metricsRepository) {
    this.metricsRepository = metricsRepository;

    this.transportClient = transportClient;
    this.storeName = storeName;
    this.needSchemaReader = needSchemaReader;

    if (needSchemaReader) {
      stats = new ClientStats(metricsRepository, AbstractAvroStoreClient.VENICE_CLIENT_NAME);
    } else {
      stats = new ClientStats(TehutiUtils.getMetricsRepository(AbstractAvroStoreClient.VENICE_CLIENT_NAME),
                              "schema_reader");
    }

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
    byte[] serializedKey = keySerializer.serialize(key);
    String requestPath = getRequestPathByStoreKey(serializedKey);

    return transportClient.get(requestPath, new ClientHttpCallback() {
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

      @Override
      public void executeOnError(int httpStatus) {
        stats.recordHttpRequest(httpStatus);
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

  protected MetricsRepository getMetricsRepository() {
    return this.metricsRepository;
  }

  public String toString() {
    return this.getClass().getSimpleName() +
        "(storeName: " + storeName +
        ", transportClient: " + transportClient.toString() + ")";
  }
}
