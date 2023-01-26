package com.linkedin.davinci.client;

/**
 * Customized transformation function for object cache.
 *
 * When the customized function is on with object cache feature is enabled,
 * DaVinci will cache the transformed object instead of the raw value.
 */
public interface CacheValueTransformer<V, T> {
  T transform(V value);
}
