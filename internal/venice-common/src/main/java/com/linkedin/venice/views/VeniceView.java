package com.linkedin.venice.views;

import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Collections;
import java.util.Map;


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

  // TODO: Add things like apply event interface (holding off on that for now)

}
