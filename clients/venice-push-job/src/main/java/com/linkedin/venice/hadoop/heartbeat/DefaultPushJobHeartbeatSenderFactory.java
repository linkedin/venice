package com.linkedin.venice.hadoop.heartbeat;

import static com.linkedin.venice.ConfigKeys.KAFKA_BOOTSTRAP_SERVERS;
import static com.linkedin.venice.status.BatchJobHeartbeatConfigs.HEARTBEAT_INITIAL_DELAY_CONFIG;
import static com.linkedin.venice.status.BatchJobHeartbeatConfigs.HEARTBEAT_INTERVAL_CONFIG;
import static com.linkedin.venice.status.BatchJobHeartbeatConfigs.HEARTBEAT_LAST_HEARTBEAT_IS_DELETE_CONFIG;
import static com.linkedin.venice.status.BatchJobHeartbeatConfigs.HEARTBEAT_STORE_NAME_CONFIG;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.VenicePushJob;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.SystemTime;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriter;
import com.linkedin.venice.writer.VeniceWriterFactory;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.avro.Schema;
import org.apache.commons.lang.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class DefaultPushJobHeartbeatSenderFactory implements PushJobHeartbeatSenderFactory {
  private static final Logger LOGGER = LogManager.getLogger(DefaultPushJobHeartbeatSenderFactory.class);

  @Override
  public PushJobHeartbeatSender createHeartbeatSender(
      VeniceProperties properties,
      @Nonnull ControllerClient controllerClient,
      Optional<Properties> sslProperties) {
    Validate.notNull(controllerClient);
    final String heartbeatStoreName = getHeartbeatStoreName(properties);
    int retryAttempts = properties.getInt(VenicePushJob.CONTROLLER_REQUEST_RETRY_ATTEMPTS, 3);
    VersionCreationResponse versionCreationResponse = ControllerClient.retryableRequest(
        controllerClient,
        retryAttempts,
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
            -1));

    if (versionCreationResponse.isError()) {
      LOGGER.warn(
          "Got error in [heartbeat store: {}] VersionCreationResponse: {}",
          heartbeatStoreName,
          versionCreationResponse);
      throw new VeniceException(versionCreationResponse.getError());
    }
    LOGGER.info("Got [heartbeat store: {}] VersionCreationResponse: {}", heartbeatStoreName, versionCreationResponse);
    String heartbeatKafkaTopicName = versionCreationResponse.getKafkaTopic();
    VeniceWriter<byte[], byte[], byte[]> veniceWriter = getVeniceWriter(
        versionCreationResponse,
        getVeniceWriterProperties(sslProperties, versionCreationResponse.getKafkaBootstrapServers()));
    Schema heartbeatKeySchema = getHeartbeatKeySchema(controllerClient, retryAttempts, heartbeatStoreName);
    Map<Integer, Schema> valueSchemasById =
        getHeartbeatValueSchemas(controllerClient, retryAttempts, heartbeatStoreName);

    final DefaultPushJobHeartbeatSender defaultPushJobHeartbeatSender = new DefaultPushJobHeartbeatSender(
        Duration.ofMillis(
            properties.getLong(
                HEARTBEAT_INITIAL_DELAY_CONFIG.getConfigName(),
                HEARTBEAT_INITIAL_DELAY_CONFIG.getDefaultValue() == null
                    ? 0
                    : HEARTBEAT_INITIAL_DELAY_CONFIG.getDefaultValue())),
        Duration.ofMillis(
            properties.getLong(
                HEARTBEAT_INTERVAL_CONFIG.getConfigName(),
                HEARTBEAT_INTERVAL_CONFIG.getDefaultValue() == null ? 0 : HEARTBEAT_INTERVAL_CONFIG.getDefaultValue())),
        veniceWriter,
        heartbeatKeySchema,
        valueSchemasById,
        heartbeatKafkaTopicName,
        properties.getBoolean(
            HEARTBEAT_LAST_HEARTBEAT_IS_DELETE_CONFIG.getConfigName(),
            HEARTBEAT_LAST_HEARTBEAT_IS_DELETE_CONFIG.getDefaultValue()));
    LOGGER.info(
        "Successfully created a default push job heartbeat sender with heartbeat store name {}",
        heartbeatStoreName);
    return defaultPushJobHeartbeatSender;
  }

  private Properties getVeniceWriterProperties(Optional<Properties> sslProperties, String kafkaBootstrapUrl) {
    Properties veniceWriterProperties = new Properties();
    veniceWriterProperties.put(KAFKA_BOOTSTRAP_SERVERS, kafkaBootstrapUrl);

    if (sslProperties.isPresent()) {
      veniceWriterProperties.putAll(sslProperties.get());
    }
    return veniceWriterProperties;
  }

  private Map<Integer, Schema> getHeartbeatValueSchemas(
      ControllerClient controllerClient,
      int retryAttempts,
      String heartbeatStoreName) {
    MultiSchemaResponse valueSchemaResponse = ControllerClient
        .retryableRequest(controllerClient, retryAttempts, c -> c.getAllValueSchema(heartbeatStoreName));
    return Arrays.stream(valueSchemaResponse.getSchemas())
        .collect(Collectors.toMap(MultiSchemaResponse.Schema::getId, schema -> Schema.parse(schema.getSchemaStr())));
  }

  private Schema getHeartbeatKeySchema(
      ControllerClient controllerClient,
      int retryAttempts,
      String heartbeatStoreName) {
    SchemaResponse keySchemaResponse =
        ControllerClient.retryableRequest(controllerClient, retryAttempts, c -> c.getKeySchema(heartbeatStoreName));
    LOGGER.info("Got [heartbeat store: {}] SchemaResponse for key schema: {}", heartbeatStoreName, keySchemaResponse);
    return Schema.parse(keySchemaResponse.getSchemaStr());
  }

  private String getHeartbeatStoreName(VeniceProperties properties) {
    return properties.getString(HEARTBEAT_STORE_NAME_CONFIG.getConfigName());
  }

  protected VeniceWriter<byte[], byte[], byte[]> getVeniceWriter(
      VersionCreationResponse versionCreationResponse,
      Properties veniceWriterProperties) {
    Properties partitionerProperties = new Properties();
    partitionerProperties.putAll(versionCreationResponse.getPartitionerParams());
    VenicePartitioner venicePartitioner = PartitionUtils.getVenicePartitioner(
        versionCreationResponse.getPartitionerClass(),
        versionCreationResponse.getAmplificationFactor(),
        new VeniceProperties(partitionerProperties));

    return new VeniceWriterFactory(veniceWriterProperties).createVeniceWriter(
        new VeniceWriterOptions.Builder(versionCreationResponse.getKafkaTopic()).setTime(new SystemTime())
            .setPartitioner(venicePartitioner)
            .setPartitionCount(versionCreationResponse.getPartitions())
            .build());
  }
}
