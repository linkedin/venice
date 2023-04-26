package com.linkedin.venice.views;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;


public class ChangeCaptureView extends VeniceView {
  public static final String CHANGE_CAPTURE_TOPIC_SUFFIX = "_cc";
  public static final String CHANGE_CAPTURE_VIEW_WRITER_CLASS_NAME =
      "com.linkedin.davinci.store.view.ChangeCaptureViewWriter";

  public ChangeCaptureView(Properties props, Store store, Map<String, String> viewParameters) {
    super(props, store, viewParameters);
  }

  @Override
  public Map<String, VeniceProperties> getTopicNamesAndConfigsForVersion(int version) {
    // No special properties for this view, so just wrap the passed in properties and pass it back.
    VeniceProperties properties = new VeniceProperties(props);
    return Collections
        .singletonMap(Version.composeKafkaTopic(store.getName(), version) + CHANGE_CAPTURE_TOPIC_SUFFIX, properties);
  }

  @Override
  public String getWriterClassName() {
    return CHANGE_CAPTURE_VIEW_WRITER_CLASS_NAME;
  }

  @Override
  public void validateConfigs() {
    super.validateConfigs();
    // change capture requires chunking to be enabled. This is because it's logistically difficult to insist
    // that all rows be under 50% the chunking threshhold (since we have to publish the before and after image of the
    // record to the change capture topic). So we make a blanket assertion.
    if (!store.isChunkingEnabled()) {
      throw new VeniceException("Change capture view are not supported with stores that don't have chunking enabled!");
    }
  }

}
