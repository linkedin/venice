package com.linkedin.venice.views;

import com.linkedin.venice.client.change.capture.protocol.RecordChangeEvent;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.PartitionerConfig;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.serialization.VeniceKafkaSerializer;
import com.linkedin.venice.serialization.avro.VeniceAvroKafkaSerializer;
import com.linkedin.venice.utils.PartitionUtils;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriterOptions;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;


public class ChangeCaptureView extends VeniceView {
  public static final String CHANGE_CAPTURE_TOPIC_SUFFIX = "_cc";
  public static final String CHANGE_CAPTURE_VIEW_WRITER_CLASS_NAME =
      "com.linkedin.davinci.store.view.ChangeCaptureViewWriter";

  public ChangeCaptureView(Properties props, String storeName, Map<String, String> viewParameters) {
    super(props, storeName, viewParameters);
  }

  @Override
  public VeniceWriterOptions.Builder getWriterOptionsBuilder(String viewTopicName, Version version) {
    VeniceWriterOptions.Builder configBuilder = new VeniceWriterOptions.Builder(viewTopicName);
    VeniceKafkaSerializer valueSerializer = new VeniceAvroKafkaSerializer(RecordChangeEvent.getClassSchema());
    configBuilder.setValuePayloadSerializer(valueSerializer);

    // Set writer properties based on the store version config
    PartitionerConfig partitionerConfig = version.getPartitionerConfig();

    if (partitionerConfig != null) {
      // TODO: It would make sense to give the option to set a different partitioner for this view. Might
      // want to consider adding it as a param available to this view type.
      VenicePartitioner venicePartitioner = PartitionUtils.getVenicePartitioner(partitionerConfig);
      configBuilder.setPartitioner(venicePartitioner);
    }

    configBuilder.setChunkingEnabled(version.isChunkingEnabled());
    return configBuilder;
  }

  @Override
  public Map<String, VeniceProperties> getTopicNamesAndConfigsForVersion(int version) {
    // No special properties for this view, so just wrap the passed in properties and pass it back.
    VeniceProperties properties = new VeniceProperties(props);
    return Collections
        .singletonMap(Version.composeKafkaTopic(storeName, version) + CHANGE_CAPTURE_TOPIC_SUFFIX, properties);
  }

  @Override
  public String composeTopicName(int version) {
    return Version.composeKafkaTopic(storeName, version) + CHANGE_CAPTURE_TOPIC_SUFFIX;
  }

  @Override
  public String getWriterClassName() {
    return CHANGE_CAPTURE_VIEW_WRITER_CLASS_NAME;
  }

  @Override
  public void validateConfigs(Store store) {
    // change capture requires A/A and chunking to be enabled. This is because it's logistically difficult to insist
    // that all rows be under 50% the chunking threshhold (since we have to publish the before and after image of the
    // record to the change capture topic). So we make a blanket assertion.
    if (!store.isActiveActiveReplicationEnabled()) {
      throw new VeniceException("Views are not supported with non Active/Active stores!");
    }
    if (!store.isChunkingEnabled()) {
      throw new VeniceException("Change capture view are not supported with stores that don't have chunking enabled!");
    }
  }

}
