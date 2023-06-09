package com.linkedin.venice.view;

import com.linkedin.venice.meta.Store;
import com.linkedin.venice.utils.VeniceProperties;
import com.linkedin.venice.views.VeniceView;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;


public class TestView extends VeniceView {
  private static TestView thisView = null;
  // The number of records processed by this view per store
  private static Map<String, AtomicInteger> storeRecordCount = new HashMap<>();

  // The number of version swaps processed by this view per store
  private static Map<String, AtomicInteger> storeVersionSwapCount = new HashMap<>();

  // The highest version encountered by this store
  private static Map<String, Integer> storeHighestVersionEncountered = new HashMap<>();

  public TestView(Properties props, Store store, Map<String, String> viewParameters) {
    super(props, store, viewParameters);
    synchronized (TestView.class) {
      if (thisView == null) {
        thisView = this;
      }
    }
  }

  @Override
  public Map<String, VeniceProperties> getTopicNamesAndConfigsForVersion(int version) {
    return Collections.emptyMap();
  }

  @Override
  public String getWriterClassName() {
    return TestViewWriter.class.getCanonicalName();
  }

  @Override
  public void close() {
    // close out anything which should be shutdown
  }

  public static TestView getInstance() {
    return thisView;
  }

  synchronized public void incrementRecordCount(String store) {
    AtomicInteger value = storeRecordCount.putIfAbsent(store, new AtomicInteger(1));
    if (value != null) {
      storeRecordCount.get(store).addAndGet(1);
    }
  }

  public int getRecordCountForStore(String store) {
    return storeRecordCount.getOrDefault(store, new AtomicInteger(0)).get();
  }

  synchronized public void incrementVersionSwapMessageCountForStore(String store) {
    AtomicInteger value = storeVersionSwapCount.putIfAbsent(store, new AtomicInteger(1));
    if (value != null) {
      storeVersionSwapCount.get(store).addAndGet(1);
    }
  }

  public int getVersionSwapCountForStore(String store) {
    return storeVersionSwapCount.getOrDefault(store, new AtomicInteger(0)).get();
  }

  synchronized public static void resetCounters() {
    if (thisView == null) {
      return;
    }
    storeVersionSwapCount.clear();
    storeRecordCount.clear();
    storeHighestVersionEncountered.clear();
  }
}
