package com.linkedin.venice.views;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
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

  public static boolean isViewTopic(String topicName) {
    // TODO: We ideally have a list of view types that exist both in OS and
    // internally. That means we need to be able to expand this list. We could
    // instantiate each list of views with reflection as we're doing in other cases
    // HOWEVER, there are too many situations related to clean up that requires
    // us to be able to determine if a left over topic (leaked) should be deleted or
    // not. If it's leaked then we may not have the relevant config to understand how
    // to instantiate the view and thus determine if the the given topic should be deleted.
    // So for now, we'll keep this static, but needs a better approach. Perhaps, a config
    // that's passed into the server that lists the types of views supported, and then
    // for each type having an uniformly named static method that doesn't override.
    return topicName.endsWith(ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX);
  }

  // TODO: see above TODO for isViewtopic function, same applies here.
  public static String parseStoreFromViewTopic(String topicName) {
    return topicName.substring(0, Version.getLastIndexOfVersionSeparator(topicName));
  }

  // TODO: see above TODO for isViewTopic function, same applies here
  public static int parseVersionFromViewTopic(String topicName) {
    int versionStartIndex = Version.getLastIndexOfVersionSeparator(topicName) + Version.VERSION_SEPARATOR.length();
    return Integer.parseInt(
        topicName.substring(versionStartIndex, topicName.lastIndexOf(ChangeCaptureView.CHANGE_CAPTURE_TOPIC_SUFFIX)));
  }
}
