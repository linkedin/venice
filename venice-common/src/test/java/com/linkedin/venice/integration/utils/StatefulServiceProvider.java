package com.linkedin.venice.integration.utils;

import java.io.File;

/**
 * A wrapper for {@link ServiceProvider} which
 * also passes in a data directory for persisting the service's state.
 *
 * @param <S> The type of {@link ProcessWrapper} returned.
 */
interface StatefulServiceProvider<S extends ProcessWrapper> extends ServiceProvider<S> {
  default S get(String serviceName, int port) throws Exception {
    File dir = TestUtils.getDataDirectory(serviceName);
    return get(serviceName, port, dir);
  }

  S get(String serviceName, int port, File dataDirectory) throws Exception;
}
