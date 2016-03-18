package com.linkedin.venice.integration.utils;

/**
 * An interface to use for passing lambdas into
 * {@link ServiceFactory#getService(String, ServiceProvider)}
 *
 * @param <S> The type of {@link ProcessWrapper} returned.
 */
interface ServiceProvider<S extends ProcessWrapper> {
  S get(String serviceName, int port) throws Exception;
}