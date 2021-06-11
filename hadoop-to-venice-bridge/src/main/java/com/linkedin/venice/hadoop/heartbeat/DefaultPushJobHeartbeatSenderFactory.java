package com.linkedin.venice.hadoop.heartbeat;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.log4j.Logger;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.status.BatchJobHeartbeatConfigs.*;

public class DefaultPushJobHeartbeatSenderFactory implements PushJobHeartbeatSenderFactory {
  private static final Logger LOGGER = Logger.getLogger(DefaultPushJobHeartbeatSenderFactory.class);

  @Override
  public PushJobHeartbeatSender createHeartbeatSender(
      VeniceProperties properties,
      ControllerClient controllerClient,
      Optional<Properties> sslProperties
  ) {
    final String heartbeatStoreName = getHeartbeatStoreName(properties);
    VersionCreationResponse versionCreationResponse = ControllerClient.retryableRequest(
            controllerClient,
            3,
            c -> c.requestTopicForWrites(
                    heartbeatStoreName,
                    1,
                    Version.PushType.STREAM,
                    "some job ID",
                    false,
                    false,
                    false,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                false,
                -1
            ));
    LOGGER.info("Got [heartbeat store: " + heartbeatStoreName + "] VersionCreationResponse: " + versionCreationResponse);
    String heartbeatKafkaTopicName = versionCreationResponse.getKafkaTopic();
    VeniceWriter<byte[], byte[], byte[]> veniceWriter = getVeniceWriter(
        versionCreationResponse,
        getVeniceWriterProperties(sslProperties, versionCreationResponse.getKafkaBootstrapServers())
    );
    Schema heartbeatKeySchema = getHeartbeatKeySchema(controllerClient, heartbeatStoreName);
    Map<Integer, Schema> valueSchemasById = getHeartbeatValueSchemas(controllerClient, heartbeatStoreName);

    return new DefaultPushJobHeartbeatSender(
            Duration.ofMillis(properties.getLong(HEARTBEAT_INITIAL_DELAY_CONFIG.getConfigName(),
                    HEARTBEAT_INITIAL_DELAY_CONFIG.getDefaultValue() == null ? 0 : HEARTBEAT_INITIAL_DELAY_CONFIG.getDefaultValue())),
            Duration.ofMillis(properties.getLong(HEARTBEAT_INTERVAL_CONFIG.getConfigName(),
                    HEARTBEAT_INTERVAL_CONFIG.getDefaultValue() == null ? 0 : HEARTBEAT_INTERVAL_CONFIG.getDefaultValue())),
            veniceWriter,
            heartbeatKeySchema,
            valueSchemasById,
            heartbeatKafkaTopicName
    );
  }

  private Properties getVeniceWriterProperties(Optional<Properties> sslProperties, String kafkaBootstrapUrl) {
    Properties veniceWriterProperties = new Properties();
    veniceWriterProperties.put(KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapUrl);

    if (sslProperties.isPresent()) {
      veniceWriterProperties.putAll(sslProperties.get());
    }
    return veniceWriterProperties;
  }

  private Map<Integer, Schema> getHeartbeatValueSchemas(ControllerClient controllerClient, String heartbeatStoreName) {
    MultiSchemaResponse valueSchemaResponse = ControllerClient.retryableRequest(
            controllerClient,
            3,
            c -> c.getAllValueSchema(heartbeatStoreName)
    );
    return Arrays.stream(valueSchemaResponse.getSchemas()).collect(
            Collectors.toMap(MultiSchemaResponse.Schema::getId, schema -> Schema.parse(schema.getSchemaStr())));
  }

  private Schema getHeartbeatKeySchema(ControllerClient controllerClient, String heartbeatStoreName) {
    SchemaResponse keySchemaResponse = ControllerClient.retryableRequest(
            controllerClient,
            3,
            c -> c.getKeySchema(heartbeatStoreName)
    );
    LOGGER.info("Got [heartbeat store: " + heartbeatStoreName + "] SchemaResponse for key schema: " + keySchemaResponse);
    return Schema.parse(keySchemaResponse.getSchemaStr());
  }

  private String getHeartbeatStoreName(VeniceProperties properties) {
    return properties.getString(HEARTBEAT_STORE_NAME_CONFIG.getConfigName());
  }

  protected VeniceWriter<byte[], byte[],byte[]> getVeniceWriter(
          VersionCreationResponse versionCreationResponse,
          Properties veniceWriterProperties
  ) {
    Properties partitionerProperties = new Properties();
    partitionerProperties.putAll(versionCreationResponse.getPartitionerParams());
    VenicePartitioner venicePartitioner = PartitionUtils.getVenicePartitioner(
        versionCreationResponse.getPartitionerClass(),
        versionCreationResponse.getAmplificationFactor(),
        new VeniceProperties(partitionerProperties)
    );
    return new VeniceWriterFactory(veniceWriterProperties).
            createBasicVeniceWriter(versionCreationResponse.getKafkaTopic(), new SystemTime(), venicePartitioner);
  }
}
