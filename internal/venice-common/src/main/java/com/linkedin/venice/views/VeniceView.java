package com.linkedin.venice.views;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;


/**
 * This is the abstract implementation of the 'management' component for a venice view.  This
 * class is meant to be lightweight and is leveraged by the controller in order to understand
 * what resources need to be maintained for the view. The first (and so far) only resource we
 * have in the interface is for KafkaTopics.  Creators of views should implement both this class
 * and it's component class {@link com.linkedin.davinci.store.view.VeniceViewWriter} and make sure
 * it's full package name is returned by
 * {@link com.linkedin.venice.views.VeniceView#getWriterClassName()}
 *
 * Ideally, all details about management and functionality for any given view is strictly contained
 * within these two classes, and as little implementation details are leaked through the rest of the
 * code base.
 *
 * TODO:
 * Despite best intentions, there is still a little bit of work to do in this interface. We've had to
 * leak some implementation details in the first cut about specific views here in order to define the
 * naming convention for resources which must be cleaned up.  Additionally, there should be support in
 * this interface for lifecycle management of arbitrary resources (not just kafka topics).
 */
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
   * Implementations should override to return a map of topicName:topic Configs relevant for topic
   * materialization (things like partition count, RF, etc.)
   * TODO: Come up with list of config names that should be parsed by VeniceAdmin to pass along to
   * TopicManager.
   * @return a map keyed by the name of the topic to be created and the configs which should be applied
   * for that creation
   */
  public Map<String, VeniceProperties> getTopicNamesAndConfigsForVersion(int version) {
    return Collections.emptyMap();
  }

  /**
   * Implementations should return the fully specified class name for the component VeniceViewWriter
   * implementation.
   *
   * @return returns the className of the writer which should be instantiated.
   */
  public String getWriterClassName() {
    throw new VeniceException("No Writer Class associated with VeniceView base class!");
  }

  /**
   * Validate that the configs set up for this view for this store are valid.  If not, throw an exception. Implementors
   * should override this function to add their own validation logic, and need only call this base implementation optionally.
   */
  public void validateConfigs() {
    // All views which publish data only work with A/A. Views which don't publish data should override this validation
    if (!store.isActiveActiveReplicationEnabled()) {
      throw new VeniceException("Views are not supported with non Active/Active stores!");
    }
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
