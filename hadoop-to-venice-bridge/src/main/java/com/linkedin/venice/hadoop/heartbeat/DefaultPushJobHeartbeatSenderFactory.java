package com.linkedin.venice.hadoop.heartbeat;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.D2ServiceDiscoveryResponse;
import com.linkedin.venice.controllerapi.MultiSchemaResponse;
import com.linkedin.venice.controllerapi.SchemaResponse;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.security.SSLFactory;
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
import javax.annotation.Nonnull;
import org.apache.avro.Schema;
import org.apache.log4j.Logger;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.status.BatchJobHeartbeatConfigs.*;

public class DefaultPushJobHeartbeatSenderFactory implements PushJobHeartbeatSenderFactory {
  private static final Logger LOGGER = Logger.getLogger(DefaultPushJobHeartbeatSenderFactory.class);

  @Override
  public PushJobHeartbeatSender createHeartbeatSender(VeniceProperties properties, Optional<SSLFactory> sslFactory) {
    final String veniceD2ZKHost = getVeniceD2ZKHost(properties);
    final String d2ServiceName = getD2ServiceName(properties);
    final String heartbeatStoreName = getHeartbeatStoreName(properties);
    final String veniceControllerUrl = getVeniceControllerUrl(properties);
    final ControllerClient controllerClient = getControllerClient(
            veniceD2ZKHost,
            d2ServiceName,
            heartbeatStoreName,
            veniceControllerUrl,
            sslFactory
    );

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
                    Optional.empty()
            ));
    LOGGER.info("Got [heartbeat store: " + heartbeatStoreName + "] VersionCreationResponse: " + versionCreationResponse);
    String heartbeatKafkaTopicName = versionCreationResponse.getKafkaTopic();
    VeniceWriter<byte[], byte[], byte[]> veniceWriter = getVeniceWriter(versionCreationResponse);
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

  private Map<Integer, Schema> getHeartbeatValueSchemas(ControllerClient controllerClient, String heartbeatStoreName) {
    MultiSchemaResponse valueSchemaResponse = ControllerClient.retryableRequest(
            controllerClient,
            3,
            c -> c.getAllValueSchema(heartbeatStoreName)
    );
    return Arrays.stream(valueSchemaResponse.getSchemas()).collect(
            Collectors.toMap(MultiSchemaResponse.Schema::getId, schema -> Schema.parse(schema.getSchemaStr())));
  }

  private ControllerClient getControllerClient(
          String veniceD2ZKHost,
          String d2ServiceName,
          String heartbeatStoreName,
          String veniceControllerUrl,
          Optional<SSLFactory> sslFactory
  ) {
    D2ServiceDiscoveryResponse discoveryResponse = ControllerClient.discoverCluster(
            veniceControllerUrl,
            heartbeatStoreName,
            sslFactory
    );
    String clusterName = discoveryResponse.getCluster();
    LOGGER.info("Found cluster: " + clusterName + " for heartbeat store: " + heartbeatStoreName);
    return new ControllerClient(clusterName, veniceControllerUrl, sslFactory);
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

  private String getD2ServiceName(VeniceProperties properties) {
    return getValueForConfigPropOrFail(HEARTBEAT_VENICE_D2_SERVICE_NAME_CONFIG.getConfigName(), properties);
  }

  private String getHeartbeatStoreName(VeniceProperties properties) {
    return getValueForConfigPropOrFail(HEARTBEAT_STORE_NAME_CONFIG.getConfigName(), properties);
  }

  private String getVeniceD2ZKHost(VeniceProperties properties) {
    return getValueForConfigPropOrFail(HEARTBEAT_VENICE_D2_ZK_HOST_CONFIG.getConfigName(), properties);
  }

  private String getVeniceControllerUrl(VeniceProperties properties) {
    return getValueForConfigPropOrFail(HEARTBEAT_STORE_NAME_CONFIG.getConfigName(), properties);
  }

  @Nonnull
  private String getValueForConfigPropOrFail(String configPropName, VeniceProperties properties) {
    String configValue = properties.getString(configPropName, () -> null);
    if (configValue == null) {
      throw new IllegalArgumentException("Expected but not found config property: " + configPropName);
    }
    return configValue;
  }

  protected VeniceWriter<byte[], byte[], byte[]> getVeniceWriter(VersionCreationResponse store) {
    Properties veniceWriterProperties = new Properties();
    veniceWriterProperties.put(KAFKA_BOOTSTRAP_SERVERS, store.getKafkaBootstrapServers());
    return getVeniceWriter(store, veniceWriterProperties);
  }

  protected VeniceWriter<byte[], byte[],byte[]> getVeniceWriter(
          VersionCreationResponse store,
          Properties veniceWriterProperties
  ) {
    Properties partitionerProperties = new Properties();
    partitionerProperties.putAll(store.getPartitionerParams());
    VenicePartitioner venicePartitioner = PartitionUtils.getVenicePartitioner(
            store.getPartitionerClass(),
            store.getAmplificationFactor(),
            new VeniceProperties(partitionerProperties)
    );
    return new VeniceWriterFactory(veniceWriterProperties).
            createBasicVeniceWriter(store.getKafkaTopic(), new SystemTime(), venicePartitioner);
  }
}
