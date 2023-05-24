package com.linkedin.venice.producer.online;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.client.store.InternalAvroStoreClient;
import com.linkedin.venice.kafka.protocol.KafkaMessageEnvelope;
import com.linkedin.venice.schema.SchemaReader;
import com.linkedin.venice.serialization.avro.AvroProtocolDefinition;
import com.linkedin.venice.service.ICProvider;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.VeniceProperties;


public class OnlineProducerFactory {
  public static <K, V> OnlineVeniceProducer<K, V> createProducer(
      ClientConfig storeClientConfig,
      VeniceProperties producerConfigs,
      ICProvider icProvider) {
    AvroGenericStoreClient<K, V> storeClient = ClientFactory.getAndStartAvroClient(storeClientConfig);

    if (!(storeClient instanceof InternalAvroStoreClient)) {
      throw new IllegalStateException(
          "Store client must be of type " + InternalAvroStoreClient.class.getCanonicalName());
    }

    ClientConfig<KafkaMessageEnvelope> kmeClientConfig = ClientConfig.cloneConfig(storeClientConfig)
        .setStoreName(AvroProtocolDefinition.KAFKA_MESSAGE_ENVELOPE.getSystemStoreName())
        .setSpecificValueClass(KafkaMessageEnvelope.class);

    SchemaReader kmeSchemaReader = ClientFactory.getSchemaReader(kmeClientConfig, icProvider);
    OnlineVeniceProducer producer = new OnlineVeniceProducer<>(
        (InternalAvroStoreClient<K, V>) storeClient,
        kmeSchemaReader,
        producerConfigs,
        storeClientConfig.getMetricsRepository(),
        icProvider);

    // KME SchemaReader is not needed after constructor. We can close it now.
    Utils.closeQuietlyWithErrorLogged(kmeSchemaReader);

    return producer;
  }
}
