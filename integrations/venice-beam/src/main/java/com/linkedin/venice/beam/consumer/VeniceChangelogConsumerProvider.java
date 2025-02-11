package com.linkedin.venice.beam.consumer;

import com.linkedin.davinci.consumer.VeniceChangelogConsumer;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;


/** Interface to provide a configured {@link VeniceChangelogConsumer} instance. */
public interface VeniceChangelogConsumerProvider<K, V> extends Serializable {
  VeniceChangelogConsumer<K, V> getVeniceChangelogConsumer(String storeName)
      throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException;

  VeniceChangelogConsumer<K, V> getVeniceChangelogConsumer(String storeName, String consumerId)
      throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException;
}
