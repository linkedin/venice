package com.linkedin.venice.integration.utils;

/**
 * An interface to use for passing lambdas into
 * {@link ServiceFactory#getService(String, ServiceProvider)}
 *
 * @param <Service> The type of {@link ProcessWrapper} returned.
 */
interface ServiceProvider<Service> {
  Service get(String serviceName) throws Exception;
}
