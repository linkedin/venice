package com.linkedin.venice.replication;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.DatastreamRestClient;
import com.linkedin.datastream.DatastreamRestClientFactory;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.connectors.kafka.KafkaConnector;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.TopicManager;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;


public class BrooklinTopicReplicator extends TopicReplicator {

  private final String brooklinConnectionString;
  private final String kafkaConnection;
  private final DatastreamRestClient client;
  private final String veniceCluster;
  private final String applicationId;
  private static final ObjectMapper mapper = new ObjectMapper();

  public static final String TRANSPORT_PROVIDER_NAME = "kafka";

  private static final Logger logger = Logger.getLogger(TopicManager.class);

  /**
   *
   * @param brooklinConnectionString For connecting to the brooklin cluster, http://host:port or d2://service
   * @param kafkaConnection For connecting to kafka brokers, host:port
   * @param topicManager TopicManager for checking information about the Kafka topics.
   * @param veniceCluster Name of the venice cluster, used to create unique datastream names
   * @param applicationId The name of the service using the BrooklinTopicReplicator, this gets used as the "owner" for any created datastreams
   */
  public BrooklinTopicReplicator(String brooklinConnectionString, String kafkaConnection, TopicManager topicManager, String veniceCluster, String applicationId){
    super(topicManager);
    this.brooklinConnectionString = brooklinConnectionString;
    this.kafkaConnection = kafkaConnection;
    this.client = DatastreamRestClientFactory.getClient(brooklinConnectionString);
    this.veniceCluster = veniceCluster;
    this.applicationId = applicationId;
  }

  @Override
  void beginReplicationInternal(String sourceTopic, String destinationTopic, int partitionCount,
      Optional<Map<Integer, Long>> startingOffsets) {

    String name = datastreamName(sourceTopic, destinationTopic);

    Datastream datastream = new Datastream();
    datastream.setName(name);
    datastream.setConnectorName(KafkaConnector.CONNECTOR_NAME);

    StringMap metadata = new StringMap();

    metadata.put(DatastreamMetadataConstants.OWNER_KEY, applicationId); // application Id
    metadata.put(DatastreamMetadataConstants.CREATION_MS, String.valueOf(Instant.now().toEpochMilli()));
    if (startingOffsets.isPresent()) {
      Map<String, Long> startingOffsetsStringMap = new HashMap<>();
      for (Map.Entry<Integer, Long> entry : startingOffsets.get().entrySet()){
        startingOffsetsStringMap.put(Integer.toString(entry.getKey()), entry.getValue());
      }
      try {
        String startingOffsetsJson = mapper.writeValueAsString(startingOffsetsStringMap);
        metadata.put(DatastreamMetadataConstants.START_POSITION, startingOffsetsJson);
      } catch (IOException e) { // This shouldn't happen
        throw new VeniceException("There was a failure parsing starting offsets map to json: " + startingOffsetsStringMap.toString(), e);
      }
    }
    datastream.setMetadata(metadata);

    DatastreamSource source  = new DatastreamSource();
    String sourceConnectionString = "kafka://" + kafkaConnection + "/" + sourceTopic;
    source.setConnectionString(sourceConnectionString);
    source.setPartitions(partitionCount);
    datastream.setSource(source);

    String destinationConnectionString = "kafka://" + kafkaConnection + "/" + destinationTopic;
    DatastreamDestination destination = new DatastreamDestination();
    destination.setConnectionString(destinationConnectionString);
    destination.setPartitions(partitionCount);
    datastream.setDestination(destination);
    datastream.setTransportProviderName(TRANSPORT_PROVIDER_NAME);


    client.createDatastream(datastream);
    try {
      client.waitTillDatastreamIsInitialized(datastream.getName(), Duration.ofSeconds(30).toMillis());
    } catch (InterruptedException e) {
      logger.warn("Interrupted while waiting for datastream " + name + " to be initialized.", e);
    }
  }

  @Override
  void terminateReplicationInternal(String sourceTopic, String destinationTopic) {
    String name = datastreamName(sourceTopic, destinationTopic);
    if (client.datastreamExists(name)){
      client.deleteDatastream(name);
      try {
        client.waitTillDatastreamIsDeleted(name, Duration.ofSeconds(30).toMillis());
      } catch (InterruptedException e) {
        logger.warn("Interrupted while waiting for datastream " + name + " to be deleted.", e);
      }
    } else {
      logger.error("Cannot delete brooklin replication stream named " + name + " because it does not exist");
    }
  }

  /**
   * Callers to begin or terminate replication should be able to just specify the source and destination topics.
   * This method lets us use a reliable convention for turning source and destination into a datastream name
   * to use for referring to the datastream.
   * @param source
   * @param destination
   * @return
   */
  private String datastreamName(String source, String destination){
    return Arrays.asList(veniceCluster, source, destination).stream()
        .collect(Collectors.joining("__"));
  }

  @Override
  public void close() {
    client.shutdown();
  }
}
