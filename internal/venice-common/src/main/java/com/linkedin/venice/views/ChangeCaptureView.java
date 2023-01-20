package com.linkedin.venice.views;

import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Collections;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.kafka.clients.consumer.ConsumerRecord;


public class ChangeCaptureView extends VeniceView {
  public final static String CHANGE_CAPTURE_TOPIC_SUFFIX = "_cc";

  public ChangeCaptureView(VeniceProperties props, Store store) {
    super(props, store);
  }

  @Override
  public Map<String, VeniceProperties> getTopicNamesAndConfigsForVersion(int version) {
    return Collections.singletonMap(
        Version.composeKafkaTopic(store.getName(), version) + CHANGE_CAPTURE_TOPIC_SUFFIX,
        new VeniceProperties());
  }

  @Override
  public void processRecord(ConsumerRecord record, int version, Schema keySchema, Schema valueSchema) {
    // query local transientRecord/rockdb for current value.
  }

}
