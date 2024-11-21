package com.linkedin.venice.beam.consumer;

import com.linkedin.davinci.consumer.VeniceChangelogConsumer;
import com.linkedin.davinci.consumer.VeniceChangelogConsumerClientFactory;
import java.lang.reflect.InvocationTargetException;


/** Provides a configured {@link VeniceChangelogConsumer} instance. */
public class LocalVeniceChangelogConsumerProvider<K, V> implements VeniceChangelogConsumerProvider<K, V> {
  private static final long serialVersionUID = 1L;

  private final Class<? extends VeniceChangelogConsumerClientFactory> _veniceChangelogConsumerClientFactoryClass;

  public LocalVeniceChangelogConsumerProvider(
      Class<? extends VeniceChangelogConsumerClientFactory> veniceChangelogConsumerClientFactoryClass) {
    _veniceChangelogConsumerClientFactoryClass = veniceChangelogConsumerClientFactoryClass;
  }

  @Override
  public VeniceChangelogConsumer<K, V> getVeniceChangelogConsumer(String storeName)
      throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        this._veniceChangelogConsumerClientFactoryClass.getDeclaredConstructor().newInstance();
    return veniceChangelogConsumerClientFactory.getChangelogConsumer(storeName);
  }

  @Override
  public VeniceChangelogConsumer<K, V> getVeniceChangelogConsumer(String storeName, String consumerId)
      throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
    VeniceChangelogConsumerClientFactory veniceChangelogConsumerClientFactory =
        this._veniceChangelogConsumerClientFactoryClass.getDeclaredConstructor().newInstance();
    return veniceChangelogConsumerClientFactory.getChangelogConsumer(storeName, consumerId);
  }
}
