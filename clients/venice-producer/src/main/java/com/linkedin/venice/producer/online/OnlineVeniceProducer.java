package com.linkedin.venice.producer.online;

import static com.linkedin.venice.ConfigKeys.CLIENT_PRODUCER_SCHEMA_REFRESH_INTERVAL_SECONDS;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.client.store.InternalAvroStoreClient;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.producer.AbstractVeniceProducer;
import com.linkedin.venice.producer.VeniceProducer;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.utils.ObjectMapperFactory;
import com.linkedin.venice.utils.RetryUtils;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * An implementation of a VeniceProducer suitable for online applications. This producer only supports producing data to
 * a store's RT topic.
 *
 * @see AbstractVeniceProducer
 * @see VeniceProducer
 */
public class OnlineVeniceProducer<K, V> extends AbstractVeniceProducer<K, V> {
  private static final Logger LOGGER = LogManager.getLogger(OnlineVeniceProducer.class);
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();

  private final String storeName;
  private final InternalAvroStoreClient<K, V> storeClient;

  private final SchemaReader schemaReader;
  private final ICProvider icProvider;

  OnlineVeniceProducer(
      ClientConfig storeClientConfig,
      VeniceProperties producerConfigs,
      MetricsRepository metricsRepository,
      ICProvider icProvider) {
    LOGGER.info("Creating venice online producer for: {}", storeClientConfig.getStoreName());
    this.storeName = storeClientConfig.getStoreName();
    this.icProvider = icProvider;

    Duration schemaRefreshPeriod;
    if (producerConfigs.containsKey(CLIENT_PRODUCER_SCHEMA_REFRESH_INTERVAL_SECONDS)) {
      schemaRefreshPeriod =
          Duration.ofSeconds(producerConfigs.getLong(CLIENT_PRODUCER_SCHEMA_REFRESH_INTERVAL_SECONDS));
    } else {
      schemaRefreshPeriod = storeClientConfig.getSchemaRefreshPeriod();
    }

    ClientConfig clientConfigForSchemaReader = ClientConfig.cloneConfig(storeClientConfig)
        .setSchemaRefreshPeriod(schemaRefreshPeriod)
        .setMetricsRepository(null);

    // We don't need metrics and schema refresh for KME schema reader
    ClientConfig<KafkaMessageEnvelope> kmeClientConfig = ClientConfig.cloneConfig(clientConfigForSchemaReader)
        .setStoreName(AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getSystemStoreName())
        .setSpecificValueClass(KafkaMessageEnvelope.class)
        .setSchemaRefreshPeriod(null);

    // We don't need metrics and schema refresh for the client that is used for requestTopic call
    ClientConfig clientConfigForRouterQueries = ClientConfig.cloneConfig(storeClientConfig)
        .setSchemaRefreshPeriod(schemaRefreshPeriod)
        .setMetricsRepository(null)
        .setSchemaRefreshPeriod(null);

    try {
      AvroGenericStoreClient<K, V> tempStoreClient = ClientFactory.getAndStartAvroClient(clientConfigForRouterQueries);

      if (!(tempStoreClient instanceof InternalAvroStoreClient)) {
        throw new IllegalStateException(
            "Store client must be of type " + InternalAvroStoreClient.class.getCanonicalName() + ". Got "
                + getClass().getCanonicalName());
      }

      this.storeClient = (InternalAvroStoreClient<K, V>) tempStoreClient;
      this.schemaReader = ClientFactory.getSchemaReader(clientConfigForSchemaReader, icProvider);
      try (SchemaReader kmeSchemaReader = ClientFactory.getSchemaReader(kmeClientConfig, icProvider)) {
        configure(storeName, producerConfigs, metricsRepository, schemaReader, kmeSchemaReader);
      }
    } catch (Throwable e) {
      // When using D2TransportClient, the threads that D2 creates are non-daemon threads which hold up graceful
      // termination of the "main" thread. This is especially problematic for the shell producer.
      Utils.closeQuietlyWithErrorLogged(this);
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      } else {
        throw new VeniceException(e);
      }
    }
    LOGGER.info(
        "Created venice online producer for: {}{}",
        storeName,
        needsPartitionRouting ? " with partition routing" : "");
  }

  @Override
  protected VersionCreationResponse requestTopic() {
    String requestTopicRequestPath = "request_topic/" + storeName;
    VersionCreationResponse versionCreationResponse;
    byte[] response = executeRouterRequest(requestTopicRequestPath);
    try {
      versionCreationResponse = OBJECT_MAPPER.readValue(response, VersionCreationResponse.class);
    } catch (Exception e) {
      throw new VeniceException("Got exception while deserializing response", e);
    }
    if (versionCreationResponse.isError()) {
      throw new VeniceException(
          "Received an error while fetching metadata from path: " + requestTopicRequestPath + ", error message: "
              + versionCreationResponse.getError());
    }
    return versionCreationResponse;
  }

  private byte[] executeRouterRequest(String requestPath) {
    // Defensive coding in case some thread enters this before the executor is closed.
    if (isClosed()) {
      throw new VeniceException("Client is closed. Refusing to make any more requests.");
    }

    byte[] response;
    try {
      CompletableFuture<byte[]> responseFuture;
      if (icProvider != null) {
        responseFuture = icProvider.call(this.getClass().getCanonicalName(), () -> storeClient.getRaw(requestPath));
      } else {
        responseFuture = storeClient.getRaw(requestPath);
      }
      response = RetryUtils.executeWithMaxAttempt(
          () -> responseFuture.get(),
          3,
          Duration.ofSeconds(1),
          Collections.singletonList(ExecutionException.class));
    } catch (Exception e) {
      throw new VeniceException("Failed to execute request from path " + requestPath, e);
    }

    if (response == null) {
      throw new VeniceException("Requested data doesn't exist for request path: " + requestPath);
    }
    return response;
  }

  @Override
  public void close() throws IOException {
    if (!isClosed()) {
      super.close();
      Utils.closeQuietlyWithErrorLogged(schemaReader);
      Utils.closeQuietlyWithErrorLogged(storeClient);
    }
  }
}
