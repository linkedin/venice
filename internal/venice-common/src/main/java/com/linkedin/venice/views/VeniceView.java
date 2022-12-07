package com.linkedin.venice.views;

import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Collections;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.kafka.clients.consumer.ConsumerRecord;


public abstract class VeniceView {
  protected final VeniceProperties props;
  protected final Store store;

  public VeniceView(VeniceProperties props, Store store) {
    this.props = props;
    this.store = store;
    // based on props, lazily instantiates instances of a writer/manager classes for
    // generating resources and executing on change events. We'll build writer/manager
  }

  /**
   * Implementations override to return a map of topicName:topic Configs relevant for topic materialization (things like partition count, RF, etc.)
   * TODO: Come up with list of config names that should be parsed by VeniceAdmin to pass along to TopicManager.
   * @return
   */
  public Map<String, VeniceProperties> getTopicNamesAndConfigsForVersion(int version) {
    return Collections.emptyMap();
  }

  // TODO: This might not be the final interface for this, but it stands to reason that a view might have to
  // deserialize a key/value in the record and perform some action based on the value of certain fields.
  public void processRecord(ConsumerRecord record, int version, Schema keySchema, Schema valueSchema) {
    // query local transientRecord/rockdb for current value.
  }
}
