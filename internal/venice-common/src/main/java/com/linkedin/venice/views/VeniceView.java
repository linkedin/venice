package com.linkedin.venice.views;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;


public abstract class VeniceView {
  protected final Properties props;
  protected final Store store;
  protected final Map<String, String> viewParameters;

  public VeniceView(Properties props, Store store, Map<String, String> viewParameters) {
    this.props = props;
    this.store = store;
    this.viewParameters = viewParameters;
  }

  /**
   * Implementations override to return a map of topicName:topic Configs relevant for topic materialization (things like partition count, RF, etc.)
   * TODO: Come up with list of config names that should be parsed by VeniceAdmin to pass along to TopicManager.
   * @return
   */
  public Map<String, VeniceProperties> getTopicNamesAndConfigsForVersion(int version) {
    return Collections.emptyMap();
  }

  public String getWriterClassName() {
    throw new VeniceException("No Writer Class associated with VeniceView base class!");
  }

  public void close() {
    // close out anything which should be shutdown
  }
}
