package com.linkedin.venice.controller;

import static com.linkedin.venice.controller.VeniceHelixAdmin.INTERNAL_STORE_GET_RRT_TOPIC_ATTEMPTS;
import static com.linkedin.venice.controller.VeniceHelixAdmin.INTERNAL_STORE_RTT_RETRY_BACKOFF_MS;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.venice.client.store.AvroSpecificStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.common.VeniceSystemStoreUtils;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.participant.protocol.ParticipantMessageKey;
import com.linkedin.venice.participant.protocol.ParticipantMessageValue;
import com.linkedin.venice.pubsub.PubSubTopicRepository;
import com.linkedin.venice.pubsub.api.PubSubTopic;
import com.linkedin.venice.pubsub.manager.TopicManagerRepository;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.utils.Utils;
import com.linkedin.venice.utils.concurrent.VeniceConcurrentHashMap;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.io.Closeable;
import java.util.Map;
import java.util.Objects;


/**
 * A helper class that wraps the readers and writers for the participant stores.
 */
public class ParticipantStoreClientsManager implements Closeable {
  private final D2Client d2Client;
  private final String clusterDiscoveryD2ServiceName;
  protected final PubSubTopicRepository pubSubTopicRepository;
  private TopicManagerRepository topicManagerRepository;
  private final VeniceWriterFactory veniceWriterFactory;
  private final Map<String, AvroSpecificStoreClient<ParticipantMessageKey, ParticipantMessageValue>> readClients =
      new VeniceConcurrentHashMap<>();
  private final Map<String, VeniceWriter> writeClients = new VeniceConcurrentHashMap<>();

  public ParticipantStoreClientsManager(
      D2Client d2Client,
      String clusterDiscoveryD2ServiceName,
      TopicManagerRepository topicManagerRepository,
      VeniceWriterFactory veniceWriterFactory,
      PubSubTopicRepository pubSubTopicRepository) {
    this.d2Client = Objects.requireNonNull(d2Client, "ParticipantStoreClientsManager requires D2Client to be provided");
    this.clusterDiscoveryD2ServiceName = clusterDiscoveryD2ServiceName;
    this.topicManagerRepository = topicManagerRepository;
    this.veniceWriterFactory = veniceWriterFactory;
    this.pubSubTopicRepository = pubSubTopicRepository;
  }

  public AvroSpecificStoreClient<ParticipantMessageKey, ParticipantMessageValue> getReader(String clusterName) {
    return readClients.computeIfAbsent(clusterName, k -> {
      ClientConfig<ParticipantMessageValue> newClientConfig =
          ClientConfig
              .defaultSpecificClientConfig(
                  VeniceSystemStoreUtils.getParticipantStoreNameForCluster(clusterName),
                  ParticipantMessageValue.class)
              .setD2Client(d2Client)
              .setD2ServiceName(clusterDiscoveryD2ServiceName);
      return ClientFactory.getAndStartSpecificAvroClient(newClientConfig);
    });
  }

  public VeniceWriter getWriter(String clusterName) {
    return writeClients.computeIfAbsent(clusterName, k -> {
      int attempts = 0;
      boolean verified = false;
      PubSubTopic topic = pubSubTopicRepository.getTopic(
          Version.composeRealTimeTopic(VeniceSystemStoreUtils.getParticipantStoreNameForCluster(clusterName)));
      while (attempts < INTERNAL_STORE_GET_RRT_TOPIC_ATTEMPTS) {
        if (topicManagerRepository.getLocalTopicManager().containsTopicAndAllPartitionsAreOnline(topic)) {
          verified = true;
          break;
        }
        attempts++;
        Utils.sleep(INTERNAL_STORE_RTT_RETRY_BACKOFF_MS);
      }
      if (!verified) {
        throw new VeniceException(
            "Can't find the expected topic " + topic + " for participant message store "
                + VeniceSystemStoreUtils.getParticipantStoreNameForCluster(clusterName));
      }
      return veniceWriterFactory.createVeniceWriter(
          new VeniceWriterOptions.Builder(topic.getName())
              .setKeySerializer(new VeniceAvroKafkaSerializer(ParticipantMessageKey.getClassSchema().toString()))
              .setValueSerializer(new VeniceAvroKafkaSerializer(ParticipantMessageValue.getClassSchema().toString()))
              .build());
    });
  }

  @Override
  public void close() {
    for (VeniceWriter client: writeClients.values()) {
      client.close();
    }
    for (AvroSpecificStoreClient client: readClients.values()) {
      client.close();
    }
  }

  // following methods are visible for testing
  Map<String, AvroSpecificStoreClient<ParticipantMessageKey, ParticipantMessageValue>> getReadClients() {
    return readClients;
  }

  Map<String, VeniceWriter> getWriteClients() {
    return writeClients;
  }
}
