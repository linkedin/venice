package com.linkedin.venice.integration.utils;

/**
 * An interface to use for passing lambdas into
 * {@link ServiceFactory#getArbitraryService(String, ArbitraryServiceProvider)}
 *
 * @param <S> The type of {@link ProcessWrapper} returned.
 */
interface ArbitraryServiceProvider<S> {
  S get(String serviceName, int port) throws Exception;
}