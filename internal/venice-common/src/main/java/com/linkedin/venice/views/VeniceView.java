package com.linkedin.venice.views;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.writer.VeniceWriterOptions;
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
  public static final String VIEW_STORE_PREFIX = "view_store_";
  public static final String VIEW_NAME_SEPARATOR = "_";
  protected final Properties props;
  protected final String storeName;
  protected final Map<String, String> viewParameters;

  public VeniceView(Properties props, String storeName, Map<String, String> viewParameters) {
    this.props = props;
    this.storeName = storeName;
    this.viewParameters = viewParameters;
    this.props.putAll(viewParameters);
  }

  public VeniceWriterOptions.Builder getWriterOptionsBuilder(String viewTopicName, Version version) {
    throw new VeniceException("Cannot get VeniceWriterOptions.Builder with VeniceView base class!");
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

  public String composeTopicName(int version) {
    return Version.composeKafkaTopic(storeName, version);
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
   * should override this function to add their own validation logic.
   */
  public void validateConfigs(Store store) {
    // validation based on view implementation
  }

  public void close(boolean gracefulClose) {
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
    return topicName.endsWith(MaterializedView.MATERIALIZED_VIEW_TOPIC_SUFFIX);
  }

  // TODO: see above TODO for isViewtopic function, same applies here.
  public static String parseStoreFromViewTopic(String topicName) {
    return topicName.substring(0, Version.getLastIndexOfVersionSeparator(topicName));
  }

  // TODO: see above TODO for isViewTopic function, same applies here
  public static int parseVersionFromViewTopic(String topicName) {
    if (!isViewTopic(topicName)) {
      throw new VeniceException("Cannot parse version because this is not a view topic, topic name: " + topicName);
    }
    int versionStartIndex = Version.getLastIndexOfVersionSeparator(topicName) + Version.VERSION_SEPARATOR.length();
    int versionEndIndex = versionStartIndex + topicName.substring(versionStartIndex).indexOf(VIEW_NAME_SEPARATOR);
    return Integer.parseInt(topicName.substring(versionStartIndex, versionEndIndex));
  }

  /**
   * @param topicName for the view topic, e.g. batchStore_148ff3a146001_v1_MaterializedViewTest_mv
   * @return the corresponding store name and view name for the given view topic in a single string.
   */
  public static String parseStoreAndViewFromViewTopic(String topicName) {
    if (!isViewTopic(topicName)) {
      throw new IllegalArgumentException(
          "Cannot parse store and view because this is not a view topic, topic name: " + topicName);
    }
    String storeName = parseStoreFromViewTopic(topicName);
    int viewTopicSuffixIndex = topicName.lastIndexOf(VIEW_NAME_SEPARATOR);
    int viewNameStartIndex = topicName.substring(0, viewTopicSuffixIndex).lastIndexOf(VIEW_NAME_SEPARATOR);
    return VIEW_STORE_PREFIX + storeName + topicName.substring(viewNameStartIndex, viewTopicSuffixIndex);
  }

  public static String getViewStoreName(String storeName, String viewName) {
    if (isViewStore(storeName)) {
      throw new IllegalArgumentException(storeName + " is already associated with a view");
    }
    return VIEW_STORE_PREFIX + storeName + VIEW_NAME_SEPARATOR + viewName;
  }

  public static String getStoreNameFromViewStoreName(String viewStoreName) {
    if (!isViewStore(viewStoreName)) {
      throw new IllegalArgumentException(viewStoreName + " is not a view store");
    }
    String viewStoreNameWithoutPrefix = viewStoreName.substring(VIEW_STORE_PREFIX.length());
    int storeNameEndIndex = viewStoreNameWithoutPrefix.lastIndexOf(VIEW_NAME_SEPARATOR);
    return viewStoreNameWithoutPrefix.substring(0, storeNameEndIndex);
  }

  public static String getViewNameFromViewStoreName(String viewStoreName) {
    if (!isViewStore(viewStoreName)) {
      throw new IllegalArgumentException(viewStoreName + " is not a view store");
    }
    int viewNameStartIndex = viewStoreName.lastIndexOf(VIEW_NAME_SEPARATOR);
    return viewStoreName.substring(viewNameStartIndex + 1);
  }

  public static boolean isViewStore(String storeName) {
    return storeName.startsWith(VIEW_STORE_PREFIX);
  }

  /**
   * @param storeName
   * @return storeName if the provided store name is a regular Venice store name or extract the regular Venice store
   * name if the provided storeName is a view store name.
   */
  public static String getStoreName(String storeName) {
    if (isViewStore(storeName)) {
      return getStoreNameFromViewStoreName(storeName);
    } else {
      return storeName;
    }
  }
}
